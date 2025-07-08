from dataclasses import asdict
from uuid import UUID
from typing import Union
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
        return [p.replace('"', "") for p in path if p]

    if not isinstance(path, str):
        raise ValueError("path must be a string or list of strings")

    # Regex to detect bracketed list strings like: [a, b, "c d", 'e f']
    bracketed_pattern = re.compile(r'^\s*\[(.*)\]\s*$')

    m = bracketed_pattern.match(path)
    if m:
        inner = m.group(1)

        # Regex to capture segments, quoted or unquoted, separated by commas
        segment_pattern = re.compile(r'''
            \s*                         # optional leading whitespace
            (                           # capture group for segment
              "(?:[^"\\]|\\.)*"         |  # double quoted string (with escapes)
              '(?:[^'\\]|\\.)*'         |  # single quoted string (with escapes)
              [^,'" ](?:[^,]*)?            # unquoted segment (no commas or quotes)
            )
            \s*                         # optional trailing whitespace
            (?:,|$)                     # comma or end of string
        ''', re.VERBOSE)

        segments = []
        for match in segment_pattern.finditer(inner):
            segment = match.group(1)
            if (segment.startswith("'") and segment.endswith("'")) or \
               (segment.startswith('"') and segment.endswith('"')):
                segment = segment[1:-1]
            segments.append(segment)
        return segments

    # Fallback: parse dotted notation, treating quoted segments as atomic
    token_pattern = re.compile(r'''
        "(.*?)"       |   # group 1: double-quoted segment (may contain dots)
        '(.*?)'       |   # group 2: single-quoted segment (may contain dots)
        ([^.]+)           # group 3: unquoted segment (no dots allowed)
    ''', re.VERBOSE)

    tokens = []
    for match in token_pattern.finditer(path):
        dq, sq, uq = match.groups()
        if dq is not None:
            tokens.append(dq)
        elif sq is not None:
            tokens.append(sq)
        elif uq is not None:
            tokens.append(uq)
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