from dataclasses import dataclass, field
from uuid import UUID
from typing import Optional, Literal, Union
from datetime import datetime

from .custom_objects import CatalogElement
from .dremio_utils import Config, MetadataPolicy

from .accesscontrol import (
    Owner,
    AccessControlList,
    Privilege,
)


@dataclass
class Home:
    entityType: Literal["home"]
    id: UUID
    name: str
    tag: str
    children: Optional[list[CatalogElement]] = None


@dataclass
class Space:
    entityType: Literal["space"]
    id: UUID
    name: str
    tag: str
    createdAt: datetime
    children: Optional[list[CatalogElement]] = None
    owner: Optional[Owner] = None
    accessControlList: Optional[AccessControlList] = None
    permissions: Optional[list[Privilege]] = None


@dataclass
class Function:
    id: UUID
    path: list[str]
    tag: str
    type: Literal["CONTAINER"]
    createdAt: datetime


@dataclass
class Source:
    entityType: Literal["source"]
    id: UUID
    tag: str
    type: str
    name: str
    createdAt: datetime
    metadataPolicy: MetadataPolicy
    accelerationGracePeriodMs: int
    accelerationRefreshPeriodMs: int
    accelerationNeverRefresh: bool
    accelerationNeverExpire: bool
    allowCrossSourceSelection: bool
    disableMetadataValidityCheck: bool
    config: Optional[Config] = None
    checkTableAuthorizer: Optional[bool] = None
    children: Optional[list[CatalogElement]] = None
    accessControlList: Optional[AccessControlList] = None
    permissions: Optional[list[Privilege]] = None
    owner: Optional[Owner] = None
