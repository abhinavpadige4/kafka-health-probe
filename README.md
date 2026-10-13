# kafka-health-probe

Overnight Kafka cluster health probe. Checks broker connectivity and consumer
group lag, retries transient failures with exponential backoff, logs to stdout
and `probe.log`, and exits non-zero on critical failure so cron/systemd can
trigger alerts.

## Install

```bash
pip install confluent-kafka PyYAML
```

`confluent-kafka` is the primary library used (librdkafka bindings). The
script uses `AdminClient` for metadata, group offsets, and watermarks.

## Usage

```bash
python kafka_probe.py --config config.example.json
```

Options:

| Flag | Description |
|------|-------------|
| `--config PATH` | JSON or YAML config file (required) |
| `--log-file PATH` | Log file path (default: `probe.log`) |
| `--max-retries N` | Override `max_retries` from config |
| `--lag-threshold N` | Override `lag_threshold` from config |

## Config

JSON example (`config.example.json`):

```json
{
  "brokers": ["broker1:9092", "broker2:9092"],
  "topic": "orders",
  "consumer_group": "orders-consumer",
  "max_retries": 3,
  "lag_threshold": 1000,
  "timeout_sec": 10
}
```

YAML example:

```yaml
brokers:
  - broker1:9092
  - broker2:9092
topic: orders
consumer_group: orders-consumer
max_retries: 3
lag_threshold: 1000
timeout_sec: 10
```

Optional SASL/SSL keys are passed through to `AdminClient`:
`security.protocol`, `sasl.mechanism`, `sasl.username`, `sasl.password`,
`ssl.ca.location`, `ssl.certificate.location`, `ssl.key.location`.

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | All checks passed |
| 1 | Critical failure (broker unreachable or lag above threshold after retries) |
| 2 | Configuration / usage error |

## Cron example (every 5 minutes overnight)

```cron
*/5 0-6 * * * cd /opt/kafka-probe && /usr/bin/python3 kafka_probe.py --config config.json >> /var/log/kafka-probe.out 2>&1
```

## systemd timer example

`kafka-probe.service`:

```ini
[Unit]
Description=Kafka health probe

[Service]
Type=oneshot
WorkingDirectory=/opt/kafka-probe
ExecStart=/usr/bin/python3 kafka_probe.py --config config.json
```

`kafka-probe.timer`:

```ini
[Unit]
Description=Run Kafka probe every 5 minutes

[Timer]
OnCalendar=*:0/5
Persistent=true

[Install]
WantedBy=timers.target
```

## Idempotency

The script performs only read-only metadata/offset queries. It is safe to run
in a loop (e.g., every 5 minutes overnight) with no side effects on the
cluster.
