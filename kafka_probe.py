#!/usr/bin/env python3
"""
Kafka health probe script.
Checks broker connectivity, consumer group lag, and logs results.
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path

try:
    from confluent_kafka import Consumer, KafkaException, KafkaError, AdminClient
except ImportError:
    print("confluent_kafka library is required. Install with 'pip install confluent-kafka'", file=sys.stderr)
    sys.exit(1)

DEFAULT_LOG_FILE = "probe.log"

def setup_logging(log_file: str):
    logger = logging.getLogger("kafka_probe")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    # stdout handler
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    # file handler
    fh = logging.FileHandler(log_file)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger

def load_config(path: str):
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(f"Config file {path} not found")
    if p.suffix in {".json"}:
        return json.load(p.open())
    try:
        import yaml
    except ImportError:
        raise ImportError("PyYAML is required for yaml config files")
    if p.suffix in {".yml", ".yaml"}:
        return yaml.safe_load(p.open())
    raise ValueError("Unsupported config file format")

def exponential_backoff(attempt, base=1, cap=60):
    return min(cap, base * (2 ** attempt))

def check_brokers(admin: AdminClient, logger, retries=3):
    for attempt in range(retries + 1):
        try:
            metadata = admin.list_topics(timeout=10)
            brokers = metadata.brokers
            logger.info(f"Connected brokers: {[b.host for b in brokers.values()]}")
            return True
        except KafkaException as e:
            logger.warning(f"Broker check failed (attempt {attempt+1}/{retries}): {e}")
            if attempt < retries:
                time.sleep(exponential_backoff(attempt))
            else:
                logger.error("Unable to connect to any broker after retries")
                return False

def get_consumer_group_lag(admin: AdminClient, group: str, topic: str, logger, retries=3):
    for attempt in range(retries + 1):
        try:
            # fetch committed offsets for the group
            committed = admin.list_consumer_group_offsets(group, timeout=10)
            # fetch latest offsets for the topic partitions
            md = admin.list_topics(topic, timeout=10)
            partitions = md.topics[topic].partitions
            latest = {}
            for p_id in partitions:
                tp = admin.topic_partition(topic, p_id)
                low, high = admin.get_watermark_offsets(tp, timeout=10)
                latest[p_id] = high
            total_lag = 0
            for tp, offset_meta in committed.items():
                if tp.topic != topic:
                    continue
                committed_offset = offset_meta.offset
                latest_offset = latest.get(tp.partition, 0)
                lag = max(0, latest_offset - committed_offset)
                total_lag += lag
            logger.info(f"Consumer group '{group}' lag for topic '{topic}': {total_lag}")
            return total_lag
        except KafkaException as e:
            logger.warning(f"Lag check failed (attempt {attempt+1}/{retries}): {e}")
            if attempt < retries:
                time.sleep(exponential_backoff(attempt))
            else:
                logger.error("Failed to retrieve consumer group lag after retries")
                return None

def main():
    parser = argparse.ArgumentParser(description="Kafka overnight health probe")
    parser.add_argument("--config", required=True, help="Path to JSON/YAML config file")
    parser.add_argument("--log-file", default=DEFAULT_LOG_FILE, help="Log file path")
    args = parser.parse_args()

    logger = setup_logging(args.log_file)
    try:
        cfg = load_config(args.config)
    except Exception as exc:
        logger.error(f"Failed to load config: {exc}")
        sys.exit(2)

    brokers = cfg.get("brokers")
    if not brokers:
        logger.error("'brokers' list missing in config")
        sys.exit(2)

    admin_cfg = {"bootstrap.servers": ",".join(brokers)}
    admin = AdminClient(admin_cfg)

    if not check_brokers(admin, logger):
        sys.exit(1)

    group = cfg.get("consumer_group")
    topic = cfg.get("topic")
    if group and topic:
        lag = get_consumer_group_lag(admin, group, topic, logger)
        if lag is None:
            sys.exit(1)
        # define a threshold if needed
        threshold = cfg.get("lag_threshold", 0)
        if threshold and lag > threshold:
            logger.error(f"Lag {lag} exceeds threshold {threshold}")
            sys.exit(1)
    else:
        logger.info("Consumer group or topic not configured; skipping lag check")

    logger.info("Kafka health probe completed successfully")
    sys.exit(0)

if __name__ == "__main__":
    main()
