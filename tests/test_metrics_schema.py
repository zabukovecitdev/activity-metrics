def test_runtime_entrypoints_import():
    from activity_metrics.agent.main import app
    from activity_metrics.clickhouse_writer.main import cli as writer_cli
    from activity_metrics.collector.main import cli as collector_cli

    assert app is not None
    assert callable(collector_cli)
    assert callable(writer_cli)
