from activityreporter.shared.models.machine import Machine


class MachineParser:
    @staticmethod
    def parse(body: str) -> Machine:
        """The Machine in an agent's /v1/machine body. Every field is required; raises ValidationError if one is missing."""
        return Machine.model_validate_json(body)
