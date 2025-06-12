from dataclasses import asdict
from uuid import UUID
from typing import Union


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
        # Preserve blanks, just remove surrounding double quotes if any
        return [p.replace('"', "") for p in path if p]

    if not isinstance(path, str):
        raise ValueError("path must be a string or list of strings")

    # Regex to match:
    # - Single-quoted strings with escapes
    # - Or plain dot-separated unquoted segments
    token_pattern = re.compile(r"""
        '([^'\\]*(?:\\.[^'\\]*)*)' |   # Group 1: quoted
        ([^.]+)                        # Group 2: unquoted
    """, re.VERBOSE)

    tokens = []
    for match in token_pattern.finditer(path):
        quoted, unquoted = match.groups()
        if quoted is not None:
            tokens.append(quoted.replace("\\'", "'"))
        elif unquoted is not None:
            tokens.append(unquoted.replace('"', ""))  # Preserve blanks, no strip

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
