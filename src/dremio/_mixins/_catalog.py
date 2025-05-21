__all__ = ["_MixinCatalog"]  # this is like `export ...` in typescript

import copy
import requests

from ..utils.converter import to_dict
from ..models import *

from . import BaseClass


class _MixinCatalog(BaseClass):

    def get_catalog_by_id(self, id: Union[UUID, str]) -> CatalogObject:
        """Get the catalog by id.

        Parameters:
          id: catalog uuid.

        Returns:
          CatalogObject: The catalog.
        """
        obj = self._T(self._get_catalog_object(id=id))
        if type(obj) in [Dataset, Folder]:
            obj._dremio = self  # type: ignore
        return obj

    def get_catalog_by_path(self, path: Union[list[str], str]) -> CatalogObject:
        """Get the catalog by path.

        Parameters:
          path: catalog path: "A.B.C" or ["A","B","C"].

        Returns:
          CatalogObject: The catalog.
        """
        obj = self._T(self._get_catalog_object(path=path))
        if type(obj) in [Dataset, Folder]:
            obj._dremio = self  # type: ignore
        return obj

    def create_catalog_item(
        self, item: Union[NewCatalogObject, CatalogObject, dict]
    ) -> CatalogObject:
        """Create the catalog item.

        Parameters:
          item: item that should be created.

        Returns:
          CatalogObject: created catalog item from dremio.
        """
        url = f"{self.hostname}/api/v3/catalog"
        try:
            item = to_dict(item)
        except:
            pass
        if isinstance(item, dict):
            item = self._T(item, True)
        new_item = copy.deepcopy(item)
        new_item.id = None  # type: ignore
        payload = to_dict(new_item)
        response = requests.post(url, headers=self._headers, json=payload)
        self._raise_error(response)
        return self._T(response.json())

    def update_catalog_item(
        self, id: Union[UUID, str], item: Union[CatalogObject, dict]
    ) -> CatalogObject:
        """Set the catalog."""
        url = f"{self.hostname}/api/v3/catalog/{id}"
        if isinstance(item, dict):
            item = self._T(item)
        item.id = id if item.id != id else item.id  # type: ignore
        payload = to_dict(item)
        response = requests.put(url, headers=self._headers, json=payload)
        self._raise_error(response)
        return self._T(response.json())

    def update_catalog_item_by_path(
        self, path: list[str], item: Union[CatalogObject, dict]
    ) -> CatalogObject:
        """Set the catalog by path."""
        id = self.get_catalog_by_path(path).id
        return self.update_catalog_item(id, item)

    def delete_catalog_item(self, id: Union[UUID, str]) -> bool:
        """Delete the catalog item."""
        url = f"{self.hostname}/api/v3/catalog/{id}"
        response = requests.delete(url, headers=self._headers)
        self._raise_error(response)
        if response.status_code != 204:
            raise DremioError("unknown error", "", response.status_code)
        return True

    def _refresh_catalog(self, id: Union[UUID, str]) -> CatalogObject:
        """Refresh the reflections associated with the specified table.

        Args:
            id (Union[UUID, str]): catalog item id.

        Returns:
            CatalogObject: The catalog object.
        """
        url = f"{self.hostname}/api/v3/catalog/{str(id)}/refresh"
        response = requests.post(url, headers=self._headers)
        self._raise_error(response)
        print(response)
        return self.get_catalog_by_id(id)
