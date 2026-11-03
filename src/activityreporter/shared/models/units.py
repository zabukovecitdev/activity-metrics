from enum import Enum


class Units(Enum):
    """The unit of a metric, in the UCUM form that the pipeline stores."""

    PERCENT = "%"
    BYTES = "By"
    SECONDS = "s"
