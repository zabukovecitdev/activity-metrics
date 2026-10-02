def test_runtime_entrypoints_import():
    from activityreporter.agent.main import app
    from activityreporter.collector.main import cli as collector_cli
    from activityreporter.clickhouse_writer.main import cli as writer_cli

    assert app is not None
    assert callable(collector_cli)
    assert callable(writer_cli)
