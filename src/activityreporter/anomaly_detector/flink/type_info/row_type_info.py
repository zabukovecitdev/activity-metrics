from dataclasses import fields

from pyflink.common.typeinfo import Types
from pydantic import BaseModel


class RowTypeInfo:
    @staticmethod
    def build(record_type: type, field_types: dict):
        """Named row type in `record_type`'s field order, failing at import if the two drift apart."""
        if issubclass(record_type, BaseModel):
            names = list(record_type.model_fields)
        else:
            names = [field.name for field in fields(record_type)]
        if set(names) != set(field_types):
            raise RuntimeError(
                f"Field types {sorted(field_types)} are out of sync with {record_type.__name__} {sorted(names)}"
            )
        return Types.ROW_NAMED(names, [field_types[name] for name in names])
