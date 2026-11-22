from prometheus_client import Counter, Histogram


class WriterMetrics:
    """The ClickHouse writer's Prometheus metrics, served on :8000/metrics."""

    ROWS_WRITTEN = Counter(
        "activityreporter_writer_rows_written_total",
        "Rows written to ClickHouse",
        ["table"],
    )
    MALFORMED = Counter(
        "activityreporter_writer_malformed_records_total",
        "Kafka records skipped as malformed",
        ["table"],
    )
    MESSAGES_CONSUMED = Counter(
        "activityreporter_writer_messages_consumed_total",
        "Records polled from Kafka, by the table their topic is loaded into",
        ["table"],
    )
    WRITE_SECONDS = Histogram(
        "activityreporter_writer_write_seconds",
        "Time to insert a batch into ClickHouse and commit its offsets",
        ["table"],
    )
