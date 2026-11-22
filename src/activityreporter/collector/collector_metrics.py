from prometheus_client import Counter


class CollectorMetrics:
    """The collector's Prometheus counters, served on :8000/metrics."""

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
    MESSAGES_PUBLISHED = Counter(
        "activityreporter_collector_messages_published_total",
        "Records published to Kafka, by topic",
        ["topic"],
    )
