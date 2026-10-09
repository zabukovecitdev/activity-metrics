from unittest.mock import MagicMock, patch

import pytest

from activityreporter.clickhouse_writer import main
from activityreporter.clickhouse_writer.writers import WRITERS


def test_main_runs_every_writer_and_stops_them_all_when_one_fails():
    running = [MagicMock(run=MagicMock(side_effect=RuntimeError("db is down")))]
    running += [MagicMock() for _ in WRITERS[1:]]
    writer_types = [MagicMock(**{"from_settings.return_value": writer}) for writer in running]

    with patch.object(main, "WRITERS", writer_types), \
         patch.object(main, "KafkaRecordsRepository"), patch.object(main, "ClickHouseRepository"), \
         patch.object(main.signal, "signal"), pytest.raises(RuntimeError, match="db is down"):
        main.main()

    for writer in running:
        writer.run.assert_called_once()
        writer.stop.assert_called()
