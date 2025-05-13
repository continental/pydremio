import re
from .converter import path_to_list, path_to_dotted


RE = r"(FROM|JOIN)\s+([\w\d\.\"]+)(?:\s*\(?\s*(\w+)\s*\)?)?"


def sql_source_parser(sql: str) -> list:
    matches = re.findall(RE, sql, re.IGNORECASE)
    return [m[1] for m in matches]


def dependencies_list_from_sql(sql: str) -> list[list[str]]:
    return [path_to_list(s) for s in sql_source_parser(sql) if s]


def dependencies_dotted_from_sql(sql: str) -> list[str]:
    return [path_to_dotted(s) for s in sql_source_parser(sql) if s]


def cut_scheme(url: str) -> str:
    """Cuts scheme from given url.

    Args:
        url (str): Like "http://localhost".

    Returns:
        str: "localhost"
    """
    if url.startswith("http"):
        url = url[url.find("//") + 2 :]
    if "/" in url:
        url = url[: url.find("/")]
    if ":" in url:
        url = url[: url.find(":")]
    return url
