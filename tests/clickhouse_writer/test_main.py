from unittest.mock import MagicMock, patch

import pytest

from activityreporter.clickhouse_writer import main
from activityreporter.clickhouse_writer.sinks.all_sinks import SINKS


def test_main_runs_every_sink_and_stops_them_all_when_one_fails():
    running = [MagicMock(run=MagicMock(side_effect=RuntimeError("db is down")))]
    running += [MagicMock() for _ in SINKS[1:]]
    sink_types = [MagicMock(**{"from_settings.return_value": sink}) for sink in running]

    with patch.object(main, "SINKS", sink_types), patch.object(main, "start_http_server") as start_server, \
         patch.object(main, "KafkaRecordsConsumer"), patch.object(main, "ClickHouseRepository"), \
         patch.object(main.signal, "signal"), pytest.raises(RuntimeError, match="db is down"):
        main.main()

    start_server.assert_called_once_with(main.METRICS_PORT)
    for sink in running:
        sink.run.assert_called_once()
        sink.stop.assert_called()
