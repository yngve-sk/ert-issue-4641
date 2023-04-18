from typing import List, Optional, Tuple, Union


class ConfigWarning(UserWarning):
    pass


class ConfigValidationError(ValueError):
    def __init__(
        self,
        errors: Union[str, List[Union[Tuple[str, str], str]]],
        config_file: Optional[str] = None,
    ) -> None:
        self.errors: List[Tuple[str, str]] = []
        if isinstance(errors, list):
            for err in errors:
                if isinstance(err, str):
                    self.errors.append((config_file, err))
                else:
                    filename, error = err
                    self.errors.append((filename, error))
        else:
            self.errors.append([config_file, errors])
        super().__init__(";".join(self.get_error_messages()))

    def get_error_messages(self):
        files = {filename for filename, _ in self.errors}
        return [
            f"Parsing file `{file}` "
            f"resulted in the errors: "
            f"{','.join([e for f, e in self.errors if f == file])}"
            for file in files
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
