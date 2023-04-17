from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Union


class ConfigWarning(UserWarning):
    pass


class ConfigValidationError(ValueError):
    def __init__(
        self,
        errors: Union[str, List[Tuple[str, str]]],
        config_file: Optional[str] = None,
    ) -> None:
        self.errors: Dict[Optional[str], List[str]] = defaultdict(list)
        if isinstance(errors, list):
            for config_file, error in errors:
                self.errors[config_file].append(error)
        else:
            self.errors[config_file] = [errors]
        super().__init__(";".join(self.get_error_messages()))

    def get_error_messages(self):
        return [
            f"Parsing config file `{config_file}` "
            f"resulted in the errors: {','.join(error)}"
            for config_file, error in self.errors.items()
        ]

    @classmethod
    def from_collected(cls, errors: List["ConfigValidationError"]):
        return cls(
            [
                (config_file, message)
                for error in errors
                for config_file, message in error.errors
            ]
        )
