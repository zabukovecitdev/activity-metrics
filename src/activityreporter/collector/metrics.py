from prometheus_client import Counter


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
