from activityreporter.collector.clients.agent_urls import AgentUrls


def test_metrics_url_is_under_the_agent_url():
    assert AgentUrls.metrics_url("http://192.168.1.20:8080") == "http://192.168.1.20:8080/v1/metrics"
    assert AgentUrls.metrics_url("http://192.168.1.20:8080/") == "http://192.168.1.20:8080/v1/metrics"
