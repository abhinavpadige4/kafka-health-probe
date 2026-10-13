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
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
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
    if p.suffix.lower() in {".json"}:
        return json.loads(p.read_text())
    try:
        import yaml
    except ImportError:
        raise ImportError("PyYAML is required for yaml config files")
    return yaml.safe_load(p.read_text())

def exponential_backoff(attempt, base=1, factor=2, max_delay=60):
    delay = min(base * (factor ** attempt), max_delay)
    time.sleep(delay)

def check_brokers(admin: AdminClient, logger):
    try:
        metadata = admin.list_topics(timeout=10)
        brokers = metadata.brokers
        logger.info(f"Connected brokers: {list(brokers.keys())}")
        return True
    except KafkaException as e:
        logger.error(f"Failed to list brokers: {e}")
        return False

def check_consumer_group_lag(admin: AdminClient, group: str, topic: str, logger):
    try:
        # Get committed offsets for the group
        group_offsets = admin.list_consumer_group_offsets(group, timeout=10)
        # Get latest offsets for the topic partitions
        md = admin.list_topics(topic, timeout=10)
        partitions = md.topics[topic].partitions
        tp_list = [f"{topic}:{p}" for p in partitions]
        # Build TopicPartition objects
        from confluent_kafka import TopicPartition
        tps = [TopicPartition(topic, p) for p in partitions]
        # Query watermarks (latest offsets)
        watermarks = admin.get_watermark_offsets(tps, timeout=10)
        total_lag = 0
        for tp in tps:
            committed = group_offsets.get(tp)
            if committed is None:
                committed_offset = 0
            else:
                committed_offset = committed.offset
            high = watermarks[tp].high
            lag = max(high - committed_offset, 0)
            total_lag += lag
            logger.info(f"Partition {tp.partition}: committed={committed_offset}, latest={high}, lag={lag}")
        logger.info(f"Total lag for group '{group}' on topic '{topic}': {total_lag}")
        return total_lag == 0
    except KafkaException as e:
        logger.error(f"Error checking consumer group lag: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Kafka overnight health probe")
    parser.add_argument("--config", required=True, help="Path to JSON/YAML config file")
    parser.add_argument("--log-file", default=DEFAULT_LOG_FILE, help="Log file path")
    args = parser.parse_args()

    logger = setup_logging(args.log_file)
    try:
        cfg = load_config(args.config)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(2)

    brokers = cfg.get("brokers")
    if not brokers:
        logger.error("'brokers' not defined in config")
        sys.exit(2)

    admin_conf = {"bootstrap.servers": ",".join(brokers)}
    admin = AdminClient(admin_conf)

    max_retries = cfg.get("max_retries", 3)
    success = False
    for attempt in range(max_retries + 1):
        logger.info(f"Probe attempt {attempt + 1}")
        brokers_ok = check_brokers(admin, logger)
        group_ok = True
        if "consumer_group" in cfg and "topic" in cfg:
            group_ok = check_consumer_group_lag(admin, cfg["consumer_group"], cfg["topic"], logger)
        if brokers_ok and group_ok:
            success = True
            break
        if attempt < max_retries:
            exponential_backoff(attempt)
    if not success:
        logger.error("Kafka health probe failed after retries")
        sys.exit(1)
    else:
        logger.info("Kafka health probe succeeded")
        sys.exit(0)

if __name__ == "__main__":
    main()
