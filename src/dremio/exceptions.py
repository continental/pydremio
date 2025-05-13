from dataclasses import dataclass
from requests import Response
from typing import Optional, Any


@dataclass
class DremioError(Exception):
    errorMessage: str
    moreInfo: Optional[str] = None
    status_code: Optional[int] = None
    context: Optional[Any] = None
    code: Optional[Any] = None
    details: Optional[Any] = None
    stackTrace: Optional[Any] = None

    def __post_init__(self):
        super().__init__(f"{self.status_code}: {self.errorMessage}")

    @staticmethod
    def dict_factory(x):
        exclude_fields = ("status_code",)
        return {k: v for (k, v) in x if ((v is not None) and (k not in exclude_fields))}


def exept_error_409(response: Response) -> DremioError:
    if response.status_code == 409:
        return DremioError(**response.json())
    return response.json()


@dataclass
class DremioConnectorError(Exception):
    errorMessage: str
    moreInfo: Optional[str] = None

    def __post_init__(self):
        super().__init__(f"{self.errorMessage}")


class AuthError(Exception):
    pass
