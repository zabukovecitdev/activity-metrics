def test_runtime_entrypoints_import():
    from activityreporter.agent.main import app
    from activityreporter.collector.main import cli as collector_cli
    from activityreporter.metrics_writer.main import main as writer_main

    assert app is not None
    assert callable(collector_cli)
    assert callable(writer_main)
