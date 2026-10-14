from dataclasses import fields
from typing import Any

from pydantic import BaseModel


class RecordParser:
    @staticmethod
    def parse(record_type: type, data: dict[str, Any]):
        """`record_type` built from `data`, ignoring keys it doesn't know.

        A producer may add fields before every consumer is deployed with them.
        """
        if not isinstance(data, dict):
            raise TypeError(f"expected a JSON object, got {type(data).__name__}")
        if issubclass(record_type, BaseModel):
            return record_type.model_validate(data)
        known = {f.name for f in fields(record_type)}
        return record_type(**{k: v for k, v in data.items() if k in known})
