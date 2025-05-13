from .dremio_utils import *
from .dremio_objects import *
from .custom_objects import *
from .inputs import *
from .dataset import Dataset
from .folder import Folder
from typing import Any, List, Dict, Optional, Type, Union, NewType

## RESPONSES ##

CatalogObject = Union[
    Source,
    Dataset,
    Space,
    Folder,
    Function,
    Home,
    CatalogElement,
]

ContainerObject = Union[
    Source,
    Dataset,
    Space,
    Folder,
    # Function,
    Home,
]

MAP_RESP = {
    "home": Home,
    "space": Space,
    "dataset": Dataset,
    "folder": Folder,
    "function": Function,
    "source": Source,
    "dataset": Dataset,
    "file": File,
    "tags": Tags,
    "wiki": Wiki,
}

## INPUTS ##

NewCatalogObject = Union[NewFolder, NewDataset, NewSpace, NewSource]

MAP_INPUT = {
    "folder": NewFolder,
    "dataset": NewDataset,
    "space": NewSpace,
    "source": NewSource,
}
