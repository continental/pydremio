from dataclasses import dataclass, asdict
from uuid import UUID
from typing import Any, Type, Union


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
    if type(path) == str:
        path = (
            path.replace(", ", ".").replace("[", "").replace("]", "").replace("'", '"')
        )
    if type(path) == str and "." in path:
        path = path.split(".")

    # if type(path) == str and '/' in path:
    #   path = path.split('/')

    if type(path) == str:
        path = [path]

    if type(path) == list or isinstance(path, list):
        path = [path.replace('"', "").strip() for path in path]

    return [el for el in list(path) if el]


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
