from dataclasses import dataclass
from typing import Optional, TypedDict
from numpy import isin
import pyarrow


class SQLType(TypedDict):
    "This appears, if the JobResult is queried via http"
    name: str  # any sql type


class SchemaField[T: pyarrow.DataType | SQLType](TypedDict):
    "This appears, if the JobResult is queried via arrow flight"
    name: str
    type: T


class Schema[T: pyarrow.DataType | SQLType](list):
    def __init__(self, iterable: list[SchemaField[T]]):
        super().__init__(SchemaField[T](item) for item in iterable)

    def __setitem__(self, index, item):
        super().__setitem__(index, SchemaField[T](item))

    def __getitem__(self, key: int | str):
        for item in self:
            try:
                if item["name"] == key:
                    return item
            except:
                continue
        return super().__getitem__(key)  # type: ignore

    def insert(self, index, item):
        super().insert(index, SchemaField[T](item))

    def append(self, item):
        super().append(SchemaField[T](item))

    def extend(self, other):
        if isinstance(other, type(self)):
            super().extend(other)
        else:
            super().extend(SchemaField[T](item) for item in other)

    @property
    def names(self) -> list[str]:
        return [f["name"] for f in self]

    @property
    def types(self) -> list[T]:
        return [f["type"] for f in self]

    @property
    def type_names(self) -> list[str]:
        types = self.types
        if isinstance(types[0], dict):
            return [t["name"] for t in types]  # type: ignore
        return [str(t) for t in types]
