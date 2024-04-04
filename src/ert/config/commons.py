from pathlib import Path
from typing import Any, List, Tuple


class CustomDict(dict):  # type: ignore
    """Used for converting types that can not be serialized
    directly to json
    """

    def __init__(self, data: List[Tuple[Any, Any]]) -> None:
        for i, (key, value) in enumerate(data):
            if isinstance(value, Path):
                data[i] = (key, str(value))
            if isinstance(value, set):
                data[i] = (key, list(value))
        super().__init__(data)
