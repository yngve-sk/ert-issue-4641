from dataclasses import dataclass, InitVar
from typing import Protocol, Optional, List
from .lark_parser_common import FileContextToken


class MaybeWithToken(Protocol):
    token: Optional["FileContextToken"]


class MaybeWithKeywordToken(Protocol):
    keyword_token: Optional["FileContextToken"]


@dataclass
class ErrorInfo:
    filename: str
    message: str
    start_pos: Optional[int] = None
    line: Optional[int] = None
    column: Optional[int] = None
    end_line: Optional[int] = None
    end_column: Optional[int] = None
    end_pos: Optional[int] = None
    originates_from: InitVar[MaybeWithToken] = None
    originates_from_these: InitVar[List[MaybeWithToken]] = None
    originates_from_keyword: InitVar[MaybeWithKeywordToken] = None

    def __post_init__(
        self,
        originates_from: Optional[MaybeWithToken],
        originates_from_these: Optional[List[MaybeWithToken]],
        originates_from_keyword: Optional[MaybeWithKeywordToken],
    ):
        token = None
        if originates_from_keyword is not None and hasattr(
            originates_from_keyword, "keyword_token"
        ):
            token = originates_from_keyword.keyword_token
        elif originates_from is not None and hasattr(originates_from, "token"):
            token = originates_from.token
        elif originates_from_these is not None:
            tokens = [x.token for x in originates_from_these if hasattr(x, "token")]
            # Merge the token positions etc

            if len(tokens) > 0:

                token = FileContextToken.join_tokens(tokens)

        if token is not None:
            self.start_pos = token.start_pos
            self.line = token.line
            self.column = token.column
            self.end_line = token.end_line
            self.end_column = token.end_column
            self.end_pos = token.end_pos
        pass


class ConfigWarning(UserWarning):
    pass


class ConfigValidationError(ValueError):
    def __init__(self, errors: str, config_file: Optional[str] = None) -> None:
        self.config_file = config_file
        self.errors = errors
        super().__init__(
            (
                f"Parsing config file `{self.config_file}` "
                f"resulted in the errors: {self.errors}"
            )
            if self.config_file
            else f"{self.errors}"
        )

    @classmethod
    def raise_from_collected(cls, collected_errors: List[ErrorInfo]):
        if len(collected_errors) > 0:
            combined_str = ";".join([x.message for x in collected_errors])
            first_filename = next(
                x.filename for x in collected_errors if x.filename is not None
            )
            raise ConfigValidationError(
                errors=combined_str, config_file=first_filename or None
            )
