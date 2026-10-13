#!/usr/bin/env python3
"""Kafka health probe script.

Checks broker connectivity, consumer group lag, and logs results.
Configuration is loaded from a JSON or YAML file via --config.
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict

try:
    import yaml  # type: ignore
except ImportError:
    yaml = None

# Try to import confluent_kafka, fallback to kafka-python
try:
    from confluent_kafka import Consumer, KafkaException, KafkaError, AdminClient
    KAFKA_LIB = "confluent"
except Exception:
    from kafka import KafkaConsumer, KafkaAdminClient
    KAFKA_LIB = "kafka_python"


def setup_logging(log_file: Path) -> None:
    """Configure logging to stdout and a file with timestamps."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    fh = logging.FileHandler(log_file, mode="a")
    fh.setFormatter(formatter)
    logger.addHandler(fh)


def load_config(path: Path) -> Dict[str, Any]:
    """Load configuration from JSON or YAML file."""
    if not path.is_file():
        logging.error("Config file %s does not exist", path)
        sys.exit(1)
    try:
        if path.suffix.lower() in {".yaml", ".yml"}:
            if yaml is None:
                logging.error("PyYAML not installed, cannot read YAML config")
                sys.exit(1)
            return yaml.safe_load(path.read_text())
        else:
            return json.loads(path.read_text())
    except Exception as e:
        logging.exception("Failed to parse config file: %s", e)
        sys.exit(1)


def exponential_backoff(attempt: int, base: float = 1.0, cap: float = 30.0) -> float:
    """Calculate backoff time in seconds for given attempt number."""
    return min(cap, base * (2 ** attempt))


def check_brokers(admin: Any, max_retries: int = 3) -> bool:
    """Verify we can list brokers, retrying on transient errors."""
    for attempt in range(max_retries + 1):
        try:
            if KAFKA_LIB == "confluent":
                metadata = admin.list_topics(timeout=10)
                brokers = metadata.brokers
                logging.info("Brokers discovered: %s", ", ".join(str(b.id) for b in brokers.values()))
            else:
                # kafka-python AdminClient
                brokers = admin.describe_cluster()
                logging.info("Brokers discovered: %s", brokers)
            return True
        except Exception as e:
            logging.warning("Broker check failed (attempt %d): %s", attempt, e)
            if attempt < max_retries:
                time.sleep(exponential_backoff(attempt))
            else:
                logging.error("Unable to connect to brokers after %d attempts", max_retries)
                return False
    return False


def get_consumer_group_lag(admin: Any, group: str, topic: str, max_retries: int = 3) -> int:
    """Return total lag for a consumer group on a topic.
    For confluent_kafka we use the ConsumerGroup API; for kafka-python we compute manually.
    """
    for attempt in range(max_retries + 1):
        try:
            if KAFKA_LIB == "confluent":
                # Use the ConsumerGroup offset fetch API via AdminClient
                # Note: confluent_kafka does not expose a direct lag API, so we approximate.
                # We'll fetch committed offsets and compare with latest partition watermarks.
                consumer = Consumer({"bootstrap.servers": admin.config["bootstrap.servers"], "group.id": group})
                partitions = admin.list_topics(topic=topic, timeout=10).topics[topic].partitions
                total_lag = 0
                for p_id, p_meta in partitions.items():
                    tp = (topic, p_id)
                    committed = consumer.committed([tp], timeout=10)[0].offset
                    low, high = consumer.get_watermark_offsets(tp, timeout=10)
                    lag = max(0, high - committed)
                    total_lag += lag
                consumer.close()
                logging.info("Consumer group %s lag on %s: %d", group, topic, total_lag)
                return total_lag
            else:
                # kafka-python approach
                consumer = KafkaConsumer(bootstrap_servers=admin.bootstrap_servers, group_id=group, enable_auto_commit=False)
                partitions = admin.describe_topics([topic])[0].partitions
                total_lag = 0
                for p in partitions:
                    tp = (topic, p.id)
                    committed = consumer.committed(tp)
                    low, high = consumer.offsets_for_times({tp: None})[tp].offset, None
                    # kafka-python does not give high directly; use end_offsets
                    end = consumer.end_offsets([tp])[tp]
                    lag = max(0, end - (committed or 0))
                    total_lag += lag
                consumer.close()
                logging.info("Consumer group %s lag on %s: %d", group, topic, total_lag)
                return total_lag
        except Exception as e:
            logging.warning("Lag check failed (attempt %d): %s", attempt, e)
            if attempt < max_retries:
                time.sleep(exponential_backoff(attempt))
            else:
                logging.error("Unable to fetch lag after %d attempts", max_retries)
                return -1
    return -1


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka overnight health probe")
    parser.add_argument("--config", required=True, help="Path to JSON/YAML config file")
    args = parser.parse_args()

    log_path = Path("probe.log")
    setup_logging(log_path)

    cfg = load_config(Path(args.config))
    brokers = cfg.get("brokers")
    if not brokers:
        logging.error("'brokers' key missing in config")
        sys.exit(1)
    group = cfg.get("consumer_group")
    topic = cfg.get("topic")
    if not group or not topic:
        logging.error("'consumer_group' and 'topic' must be defined in config")
        sys.exit(1)

    # Initialise admin client
    if KAFKA_LIB == "confluent":
        admin = AdminClient({"bootstrap.servers": ",".join(brokers)})
    else:
        admin = KafkaAdminClient(bootstrap_servers=brokers)

    # 1. Broker connectivity
    if not check_brokers(admin):
        sys.exit(2)

    # 2. Consumer group lag
    lag = get_consumer_group_lag(admin, group, topic)
    if lag < 0:
        sys.exit(3)
    # Define a warning threshold (configurable)
    warn_threshold = cfg.get("lag_warning_threshold", 1000)
    if lag > warn_threshold:
        logging.warning("Consumer lag %d exceeds warning threshold %d", lag, warn_threshold)
        sys.exit(4)
    else:
        logging.info("Consumer lag %d within acceptable range", lag)

    logging.info("Kafka health probe completed successfully")
    sys.exit(0)

if __name__ == "__main__":
    main()
