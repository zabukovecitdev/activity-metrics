from prometheus_client import Counter, Histogram


class Metrics:
    SCRAPES = Counter(
        "activityreporter_collector_scrapes_total",
        "Agent scrapes by outcome: ok, fetch_error (no valid response) or publish_error",
        ["outcome"],
    )
    INVALID_RESPONSES = Counter(
        "activityreporter_collector_invalid_responses_total",
        "Agent responses that failed validation and went to the dead letter topic, by kind: metrics or machine",
        ["kind"],
    )
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
    MESSAGES_PUBLISHED = Counter(
        "activityreporter_collector_messages_published_total",
        "Records published to Kafka, by topic",
        ["topic"],
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
