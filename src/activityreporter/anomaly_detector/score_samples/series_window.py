from collections.abc import Iterable


class SeriesWindow:
    """The values a series (machine_id, metric name) is scored against: its last `POINTS`, in arrival order."""

    # A count, not a time span, so the window holds enough values for every detector at any sample rate.
    # One hour at the 10-second scrape interval.
    POINTS = 360

    @staticmethod
    def append(previous: Iterable[float], value: float) -> list[float]:
        """The window after `value` arrives: the previous values then `value`, cut to the last `POINTS`."""
        return [*previous, value][-SeriesWindow.POINTS:]
