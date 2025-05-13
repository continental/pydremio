from dataclasses import dataclass
from typing import Optional, Any, Literal

from .custom_objects import AccessControlList
from .dremio_utils import MetadataPolicy
from ..utils.converter import path_to_list


@dataclass
class NewFolder:
    path: list[str]
    accessControlList: Optional[AccessControlList] = None
    entityType: Literal["folder"] = "folder"

    def __post_init__(self):
        # convert if path is a dotted string
        if isinstance(self.path, str):
            self.path = path_to_list(self.path)


@dataclass
class NewDataset:
    path: list[str]
    type: Literal["PHYSICAL_DATASET", "VIRTUAL_DATASET"]
    sql: str
    id: Optional[str] = None
    accessControlList: Optional[AccessControlList] = None
    entityType: Literal["dataset"] = "dataset"

    def __post_init__(self):
        self.path = path_to_list(self.path)


@dataclass
class NewSpace:
    name: str
    accessControlList: Optional[AccessControlList] = None
    entityType: Literal["space"] = "space"


@dataclass
class NewSource:
    type: str
    name: str
    tag: Optional[str]
    config: Optional[dict[str, Any]]
    metadataPolicy: Optional[MetadataPolicy]
    accelerationGracePeriodMs: Optional[int] = None
    accelerationRefreshPeriodMs: Optional[int] = None
    accelerationNeverExpire: Optional[bool] = None
    accelerationNeverRefresh: Optional[bool] = None
    allowCrossSourceSelection: Optional[bool] = None
    disableMetadataValidityCheck: Optional[bool] = None
    entityType: Literal["source"] = "source"


@dataclass
class SQLRequest:
    sql: str
    context: Optional[list[str]] = None
