from dataclasses import dataclass, fields
from typing import Literal, Optional, Any, TypeVar, Union


@dataclass
class Message:
    message: str


T = TypeVar("T")


def cast(T: type[T], d: Any) -> T:
    if isinstance(d, list):  # if already correct type
        d = [cast(T.__args__[0], i) for i in d]  # type: ignore
    try:  # try to cast Optional, Union and Literal
        d = cast(T.__args__[0], d)  # type: ignore
    except:
        pass
    # try to cast dataclass
    if isinstance(d, dict):  # if already correct type
        d_ = {}
        for field in fields(T):  # type: ignore
            try:
                d_[field.name] = cast(field.type, d[field.name])  # type: ignore
            except:
                pass
        d = T(**d_)
    return d
