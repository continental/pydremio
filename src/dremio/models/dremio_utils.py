from dataclasses import dataclass, field
from typing import Optional, Any, Literal, Union
from uuid import UUID


@dataclass
class DatasetAccelerationRefreshPolicy:
    refreshPeriodMs: int
    gracePeriodMs: int
    method: Literal["FULL", "INCREMENTAL"]
    accelerationNeverExpire: bool
    accelerationNeverRefresh: bool
    refreshField: Optional[str] = None


@dataclass
class Config:
    accessKey: str
    secure: bool
    externalBucketlist: list[str]
    rootPath: str
    enableAsync: bool
    compatibilityMode: bool
    isCachingEnabled: bool
    maxCacheSpacePct: int
    requesterPays: bool
    enableFileStatusCheck: bool
    defaultCtasFormat: str
    isPartitionInferenceEnabled: bool
    credentialType: str


@dataclass
class StatusMessage:
    level = Literal["INFO", "WARN", "ERROR"]
    message: str


@dataclass
class State:
    status: Literal["good", "bad", "warn"]
    suggestedUserAction: str
    messages: Optional[list[StatusMessage]] = None


@dataclass
class MetadataPolicy:
    authTTLMs: int
    namesRefreshMs: int
    datasetRefreshAfterMs: int
    datasetExpireAfterMs: int
    datasetUpdateMode: Literal["PREFETCH", "PREFETCH_QUERIED", "INLINE"]
    deleteUnavailableDatasets: bool
    autoPromoteDatasets: bool


@dataclass
class File:
    entityType: Literal["file"]
    id: UUID
    path: list[str]


@dataclass
class Tags:
    tags: list[str]
    version: Union[str, int]


@dataclass
class Wiki:
    text: str
    version: int = 0
    _dataset_id: Union[UUID, None] = None
    _dremio: Any = None

    def __str__(self) -> str:
        return self.text

    def __repr__(self) -> str:
        return self.text

    def commit(self) -> "Wiki":
        return self._dremio.set_wiki(self._dataset_id, self, True)


@dataclass
class JobId:
    id: UUID
