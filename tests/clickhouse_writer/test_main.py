from unittest.mock import MagicMock, patch

import pytest

from activityreporter.clickhouse_writer import main
from activityreporter.clickhouse_writer.ingesters.all_ingesters import INGESTERS


def test_main_runs_every_ingester_and_stops_them_all_when_one_fails():
    running = [MagicMock(run=MagicMock(side_effect=RuntimeError("db is down")))]
    running += [MagicMock() for _ in INGESTERS[1:]]
    ingester_types = [MagicMock(**{"from_settings.return_value": ingester}) for ingester in running]

    with patch.object(main, "INGESTERS", ingester_types), patch.object(main, "start_http_server") as start_server, \
         patch.object(main, "KafkaRecordsConsumer"), patch.object(main, "ClickHouseRepository"), \
         patch.object(main.signal, "signal"), pytest.raises(RuntimeError, match="db is down"):
        main.main()

    start_server.assert_called_once_with(main.METRICS_PORT)
    for ingester in running:
        ingester.run.assert_called_once()
        ingester.stop.assert_called()
