from pydantic import BaseModel
from pyflink.common.typeinfo import Types


class RowTypeInfo:
    @staticmethod
    def build(record_type: type[BaseModel], field_types: dict):
        """Named row type in `record_type`'s field order, failing at import if the two drift apart."""
        names = list(record_type.model_fields)
        if set(names) != set(field_types):
            raise RuntimeError(
                f"Field types {sorted(field_types)} are out of sync with {record_type.__name__} {sorted(names)}"
            )
        return Types.ROW_NAMED(names, [field_types[name] for name in names])
