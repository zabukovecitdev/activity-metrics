from activityreporter.anomaly_detector.score_samples.series_window import SeriesWindow


def test_first_value_starts_a_window_of_one():
    assert SeriesWindow.append([], 5.0) == [5.0]


def test_newest_value_is_last():
    assert SeriesWindow.append([1.0, 2.0], 3.0) == [1.0, 2.0, 3.0]


def test_window_below_the_limit_keeps_every_value():
    previous = [float(i) for i in range(SeriesWindow.POINTS - 1)]

    assert SeriesWindow.append(previous, 1.0) == [*previous, 1.0]


def test_window_keeps_only_the_last_360_values():
    previous = [float(i) for i in range(360)]

    window = SeriesWindow.append(previous, 360.0)

    assert SeriesWindow.POINTS == 360
    assert window == [float(i) for i in range(1, 361)]


def test_window_is_counted_in_values_not_in_time():
    assert len(SeriesWindow.append([0.0] * 1000, 1.0)) == 360


def test_previous_values_are_not_modified():
    previous = [1.0, 2.0]

    SeriesWindow.append(previous, 3.0)

    assert previous == [1.0, 2.0]
