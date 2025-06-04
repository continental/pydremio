from dataclasses import dataclass, asdict
from uuid import UUID
from typing import Any, Type, Union
import re


def to_dict(d) -> dict:
    # print('obj',obj)
    d = asdict(d)
    result = {}
    for key, value in d.items():
        if isinstance(value, UUID):
            result[key] = str(value)
        if isinstance(key, str):
            if key[0] == "_" and key[1] != "_":
                continue
        result[key] = value
    return result


def path_to_list(path: Union[str, list[str]]) -> list[str]:
    if isinstance(path, list):
        return [p.strip().replace('"', "") for p in path if p]

    if not isinstance(path, str):
        raise ValueError("path must be a string or list of strings")

    # Regex to match either:
    # - single-quoted strings (with escaped quotes inside), or
    # - unquoted dot-separated segments
    token_pattern = re.compile(r"""
        '([^'\\]*(?:\\.[^'\\]*)*)' |   # Single-quoted strings with escapes
        ([^.]+)                        # Or plain unquoted segments
    """, re.VERBOSE)

    tokens = []
    for match in token_pattern.finditer(path):
        quoted, unquoted = match.groups()
        if quoted is not None:
            # Unescape single quotes
            tokens.append(quoted.replace("\\'", "'"))
        elif unquoted is not None:
            tokens.append(unquoted.strip())

    return [t for t in tokens if t]


def path_to_dotted(path: Union[list[str], str]) -> str:
    path = path_to_list(path)
    return '"' + '"."'.join(path) + '"'


def clear_at(d: dict) -> dict:
    res = {}

    for k, v in d.items():
        if k[0] == "@":
            res[k[1:]] = v
            continue
        res[k] = v

    return res
