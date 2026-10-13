#!/usr/bin/env python3
"""
kafka_probe.py — Overnight Kafka cluster health probe.

Checks:
  1. Broker connectivity (list brokers via metadata).
  2. Consumer group lag for a configurable topic/group.

Features:
  - Supports both `confluent-kafka` and `kafka-python` libraries.
  - Exponential backoff retry for transient errors.
  - Logs to stdout and `probe.log` with timestamps.
  - Non-zero exit codes for external monitoring (cron/systemd).
  - Loads broker details from a JSON or YAML config file via --config.
  - Idempotent and safe to run in a loop (e.g. every 5 minutes overnight).

Exit codes:
  0  — all checks passed.
  1  — configuration error.
  2  — broker connectivity failure.
  3  — consumer group lag check failure.
  4  — consumer group lag exceeded warning threshold.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Optional third-party imports — we support both kafka-python and confluent-kafka.
# ---------------------------------------------------------------------------
try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover
    yaml = None

try:
    from confluent_kafka import Consumer, KafkaError, KafkaException  # type: ignore
    KAFKA_LIB = "confluent"
except Exception:
    try:
        from kafka import KafkaConsumer  # type: ignore
        KAFKA_LIB = "kafka-python"
    except Exception:
        KAFKA_LIB = None


# ---------------------------------------------------------------------------
# Exit codes
# ---------------------------------------------------------------------------
EXIT_OK = 0
EXIT_CONFIG = 1
EXIT_BROKERS = 2
EXIT_LAG_CHECK = 3
EXIT_LAG_HIGH = 4


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def setup_logging(log_file: Path) -> None:
    """Configure logging to stdout and a file with timestamps."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # Remove any pre-existing handlers to keep behaviour idempotent.
    for h in list(logger.handlers):
        logger.removeHandler(h)

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    fh.setFormatter(formatter)
    logger.addHandler(fh)


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------
def load_config(path: Path) -> Dict[str, Any]:
    """Load configuration from a JSON or YAML file."""
    if not path.is_file():
        logging.error("Config file %s does not exist", path)
        sys.exit(EXIT_CONFIG)

    try:
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        logging.error("Cannot read config file %s: %s", path, e)
        sys.exit(EXIT_CONFIG)

    try:
        if path.suffix.lower() in {".yaml", ".yml"}:
            if yaml is None:
                logging.error("PyYAML is not installed; cannot parse YAML config")
                sys.exit(EXIT_CONFIG)
            cfg = yaml.safe_load(text)
        else:
            cfg = json.loads(text)
    except Exception as e:
        logging.error("Failed to parse config file %s: %s", path, e)
        sys.exit(EXIT_CONFIG)

    if not isinstance(cfg, dict):
        logging.error("Config root must be a mapping/object")
        sys.exit(EXIT_CONFIG)
    return cfg


# ---------------------------------------------------------------------------
# Retry helper
# ---------------------------------------------------------------------------
def exponential_backoff(attempt: int, base: float = 1.0, cap: float = 30.0) -> float:
    """Return the sleep duration (seconds) for a given retry attempt."""
    return min(cap, base * (2 ** attempt))


def with_retries(
    fn,
    *,
    max_retries: int = 3,
    base: float = 1.0,
    cap: float = 30.0,
    label: str = "operation",
):
    """Call `fn` with exponential backoff on exception.

    Returns the result of `fn` on success, or raises the last exception.
    """
    last_exc: Optional[BaseException] = None
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001 — we intentionally catch broadly here
            last_exc = e
            logging.warning(
                "%s failed (attempt %d/%d): %s", label, attempt + 1, max_retries + 1, e
            )
            if attempt < max_retries:
                sleep = exponential_backoff(attempt, base=base, cap=cap)
                logging.info("Retrying %s in %.1fs", label, sleep)
                time.sleep(sleep)
    assert last_exc is not None
    raise last_exc


# ---------------------------------------------------------------------------
# Broker connectivity check
# ---------------------------------------------------------------------------
def check_brokers_confluent(bootstrap_servers: str, timeout: int = 10) -> List[int]:
    """Return list of broker IDs using confluent-kafka Consumer metadata."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "_probe_group",
            "enable.auto.commit": "false",
            "enable.auto.offset.store": "false",
        }
    )
    try:
        metadata = consumer.list_topics(timeout=timeout)
        broker_ids = sorted(metadata.brokers.keys())
        return broker_ids
    finally:
        consumer.close()


def check_brokers_kafka_python(bootstrap_servers: List[str], timeout: int = 10) -> List[int]:
    """Return list of broker IDs using kafka-python KafkaConsumer metadata."""
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id="_probe_group",
        enable_auto_commit=False,
        request_timeout_ms=timeout * 1000,
    )
    try:
        metadata = consumer._client.cluster
        broker_ids = sorted(metadata.brokers())
        return broker_ids
    finally:
        consumer.close()


def check_brokers(cfg: Dict[str, Any]) -> List[int]:
    """High-level broker check with retries. Returns list of broker IDs."""
    brokers = cfg["brokers"]
    max_retries = int(cfg.get("max_retries", 3))

    if KAFKA_LIB == "confluent":
        bootstrap = ",".join(brokers)
        return with_retries(
            lambda: check_brokers_confluent(bootstrap),
            max_retries=max_retries,
            label="broker connectivity",
        )
    elif KAFKA_LIB == "kafka-python":
        return with_retries(
            lambda: check_brokers_kafka_python(brokers),
            max_retries=max_retries,
            label="broker connectivity",
        )
    else:
        logging.error("No supported Kafka library installed (need confluent-kafka or kafka-python)")
        sys.exit(EXIT_CONFIG)


# ---------------------------------------------------------------------------
# Consumer group lag
# ---------------------------------------------------------------------------
def _lag_confluent(
    bootstrap_servers: str, group: str, topic: str, timeout: int = 10
) -> Tuple[int, Dict[int, int]]:
    """Return (total_lag, per_partition_lag) using confluent-kafka."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group,
            "enable.auto.commit": "false",
            "enable.auto.offset.store": "false",
        }
    )
    try:
        metadata = consumer.list_topics(topic, timeout=timeout)
        partitions = metadata.topics[topic].partitions
        per_partition: Dict[int, int] = {}
        total = 0
        for p_id in partitions:
            tp = (topic, p_id)
            committed = consumer.committed([tp], timeout=timeout)[0].offset
            _, high = consumer.get_watermark_offsets(tp, timeout=timeout)
            if committed is None or committed < 0:
                # Group has no committed offset for this partition.
                lag = high
            else:
                lag = max(0, high - committed)
            per_partition[p_id] = lag
            total += lag
        return total, per_partition
    finally:
        consumer.close()


def _lag_kafka_python(
    bootstrap_servers: List[str], group: str, topic: str, timeout: int = 10
) -> Tuple[int, Dict[int, int]]:
    """Return (total_lag, per_partition_lag) using kafka-python."""
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=group,
        enable_auto_commit=False,
        request_timeout_ms=timeout * 1000,
    )
    try:
        # Discover partitions via metadata.
        metadata = consumer._client.cluster
        topic_metadata = metadata.partitions_for_topic(topic)
        if topic_metadata is None:
            raise RuntimeError(f"Topic {topic!r} not found in cluster metadata")
        per_partition: Dict[int, int] = {}
        total = 0
        for p_id in topic_metadata:
            tp = (topic, p_id)
            end_offsets = consumer.end_offsets([tp])
            high = end_offsets[tp]
            committed = consumer.committed(tp)
            if committed is None:
                lag = high
            else:
                lag = max(0, high - committed)
            per_partition[p_id] = lag
            total += lag
        return total, per_partition
    finally:
        consumer.close()


def get_consumer_group_lag(cfg: Dict[str, Any]) -> Tuple[int, Dict[int, int]]:
    """High-level lag check with retries."""
    brokers = cfg["brokers"]
    group = cfg["consumer_group"]
    topic = cfg["topic"]
    max_retries = int(cfg.get("max_retries", 3))

    if KAFKA_LIB == "confluent":
        bootstrap = ",".join(brokers)
        return with_retries(
            lambda: _lag_confluent(bootstrap, group, topic),
            max_retries=max_retries,
            label="consumer group lag",
        )
    elif KAFKA_LIB == "kafka-python":
        return with_retries(
            lambda: _lag_kafka_python(brokers, group, topic),
            max_retries=max_retries,
            label="consumer group lag",
        )
    else:
        logging.error("No supported Kafka library installed")
        sys.exit(EXIT_CONFIG)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Kafka overnight health probe (brokers + consumer group lag)."
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to a JSON or YAML config file with broker details.",
    )
    parser.add_argument(
        "--log-file",
        default="probe.log",
        help="Path to the log file (default: probe.log).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG-level logging.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    setup_logging(Path(args.log_file))
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    cfg = load_config(Path(args.config))

    # Validate required keys.
    for key in ("brokers", "consumer_group", "topic"):
        if key not in cfg or not cfg[key]:
            logging.error("Missing required config key: %r", key)
            return EXIT_CONFIG

    brokers = cfg["brokers"]
    if isinstance(brokers, str):
        brokers = [b.strip() for b in brokers.split(",") if b.strip()]
    cfg["brokers"] = brokers

    group = cfg["consumer_group"]
    topic = cfg["topic"]
    warn_threshold = int(cfg.get("lag_warning_threshold", 1000))

    logging.info(
        "Starting Kafka probe (lib=%s, brokers=%s, group=%s, topic=%s)",
        KAFKA_LIB, brokers, group, topic,
    )

    # 1. Broker connectivity.
    try:
        broker_ids = check_brokers(cfg)
    except Exception as e:
        logging.error("Broker connectivity check failed: %s", e)
        return EXIT_BROKERS
    logging.info("Broker connectivity OK — brokers: %s", broker_ids)

    # 2. Consumer group lag.
    try:
        total_lag, per_partition = get_consumer_group_lag(cfg)
    except Exception as e:
        logging.error("Consumer group lag check failed: %s", e)
        return EXIT_LAG_CHECK
    logging.info(
        "Consumer group %r lag on %r: total=%d per_partition=%s",
        group, topic, total_lag, per_partition,
    )

    # 3. Threshold check.
    if total_lag > warn_threshold:
        logging.warning(
            "Consumer lag %d exceeds warning threshold %d", total_lag, warn_threshold
        )
        return EXIT_LAG_HIGH

    logging.info("Kafka health probe completed successfully")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
