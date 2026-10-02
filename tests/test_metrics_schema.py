def test_runtime_entrypoints_import():
    from activityreporter.agent.main import app
    from activityreporter.collector.main import cli as collector_cli
    from activityreporter.clickhouse_writer.main import evaluations_cli, machines_cli, metrics_cli

    assert app is not None
    assert callable(collector_cli)
    assert callable(metrics_cli)
    assert callable(evaluations_cli)
    assert callable(machines_cli)
