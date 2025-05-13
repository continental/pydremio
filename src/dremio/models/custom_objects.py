from dataclasses import dataclass, field
import json
from uuid import UUID
from typing import Optional, Any, Literal, NewType, Union
from datetime import datetime

from .accesscontrol import Owner, AccessControlList, Privilege, Permissions


@dataclass
class CatalogElement:
    id: UUID
    path: list[str]
    type: Literal["CONTAINER", "DATASET"]  # |
    tag: Optional[str] = None
    createdAt: Optional[datetime] = None
    containerType: Optional[
        Literal["SPACE", "SOURCE", "FOLDER", "HOME", "FUNCTION"]
    ] = None
    datasetType: Optional[Literal["VIRTUAL", "PROMOTED", "DIRECT"]] = None


@dataclass
class DatasetFormat:
    type: Literal["Text", "JSON", "Parquet", "Excel", "XLS", "Delta", "Iceberg"]
    fieldDelimiter: Optional[str] = None
    lineDelimiter: Optional[str] = None
    quote: Optional[str] = None
    comment: Optional[str] = None
    escape: Optional[str] = None
    skipFirstLine: Optional[bool] = None
    extractHeader: Optional[bool] = None
    trimHeader: Optional[bool] = None
    autoGenerateColumnNames: Optional[bool] = None


@dataclass
class Filter:
    condition: Literal["LIKE", "=", "IN"]
    field: str
    value: str
    negation: Optional[bool]


@dataclass
class Filters:
    multiple: bool
    filters: list[Filter]
    operator: Optional[Literal["AND", "OR"]]


@dataclass
class DremioQuery:
    from_condition: str
    select_condition: str
    limit: int
    offset: int
    sort_order: Literal["asc", "desc"]
    where_condition: Optional[Filters]
    jobId: Optional[str]
    sort_property: Optional[str]
