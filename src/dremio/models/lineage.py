from dataclasses import dataclass
from typing import Optional
from uuid import UUID

from .custom_objects import CatalogElement


@dataclass
class Lineage:
    sources: list[CatalogElement]
    parents: list[CatalogElement]
    children: list[CatalogElement]
