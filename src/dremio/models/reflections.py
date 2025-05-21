from dataclasses import dataclass
from typing import Literal, Optional, List
from enum import Enum
from datetime import datetime
from uuid import UUID


@dataclass
class DimensionField:
    name: str
    granularity: Optional[str] = None


@dataclass
class Field:
    name: str


@dataclass
class MeasureField:
    name: str
    measureTypeList: List[
        Literal["COUNT", "SUM", "APPROX_COUNT_DISTINCT", "MIN", "MAX", "UNKNOWN"]
    ]


@dataclass
class Status:
    config: Literal["OK", "INVALID"]
    refresh: Literal["GIVEN_UP", "MANUAL", "RUNNING", "SCHEDULED"]
    availability: Literal["NONE", "INCOMPLETE", "EXPIRED", "AVAILABLE"]
    combinedStatus: Literal[
        "CAN_ACCELERATE",
        "CAN_ACCELERATE_WITH_FAILURES",
        "CANNOT_ACCELERATE_MANUAL",
        "CANNOT_ACCELERATE_SCHEDULED",
        "DISABLED",
        "EXPIRED",
        "FAILED",
        "INVALID",
        "INCOMPLETE",
        "REFRESHING",
    ]
    failureCount: int
    lastDataFetch: datetime
    expiresAt: datetime


@dataclass
class Reflection:
    id: UUID
    type: Literal["RAW", "AGGREGATION"]
    name: str
    tag: str
    createdAt: datetime
    updatedAt: datetime
    datasetId: UUID
    currentSizeBytes: int
    totalSizeBytes: int
    enabled: bool
    status: Status
    entityType: Optional[str] = None
    canAlter: Optional[bool] = None
    partitionDistributionStrategy: Optional[str] = None
    sortFields: Optional[List[Field]] = None
    partitionFields: Optional[List[Field]] = None
    distributionFields: Optional[List[Field]] = None
    arrowCachingEnabled: Optional[bool] = None
    canView: Optional[bool] = None
    dimensionFields: Optional[List[DimensionField]] = None
    measureFields: Optional[List[MeasureField]] = None
    displayFields: Optional[List[Field]] = None


@dataclass
class NewReflection:
    type: Literal["RAW", "AGGREGATION"]
    enabled: bool
    entityType: Optional[str] = None
    canAlter: Optional[bool] = None
    partitionDistributionStrategy: Optional[str] = None
    sortFields: Optional[List[Field]] = None
    partitionFields: Optional[List[Field]] = None
    distributionFields: Optional[List[Field]] = None
    arrowCachingEnabled: Optional[bool] = None
    canView: Optional[bool] = None
    dimensionFields: Optional[List[DimensionField]] = None
    measureFields: Optional[List[MeasureField]] = None
    displayFields: Optional[List[Field]] = None
