__all__ = ["_MixinRole"]  # this is like `export ...` in typescript
from typing import Any
import requests
from ..utils.converter import clear_at
from ..models import *

from . import BaseClass
from ._catalog import _MixinCatalog


class _MixinRole(_MixinCatalog, BaseClass):

    def get_role(self, id: Union[UUID, str]) -> Role:
        """Get the role.

        Parameters:
          id: role uuid.

        Returns:
          Role: role object.
        """
        url = f"{self.hostname}/api/v3/role/{id}"
        response = requests.get(url, headers=self._headers)
        self._raise_error(response)
        return cast(Role, clear_at(response.json()))

    def get_role_by_name(self, name: str) -> Role:
        """Get the role by name.

        Parameters:
          name: role name.

        Returns:
          Role: role object.
        """
        url = f"{self.hostname}/api/v3/role/by-name/{name}"
        response = requests.get(url, headers=self._headers)
        self._raise_error(response)
        return cast(Role, clear_at(response.json()))

    def create_role(
        self,
        name: str,
        roles: Union[list[Role], None] = None,
        description: Union[str, None] = None,
    ) -> Role:
        """Create the role.

        Parameters:
          name: role name.
          roles: list of roles.
          description: role description.

        Returns:
          Role: role object from dremio after creation.
        """
        url = f"{self.hostname}/api/v3/role"
        payload: dict[str, Any] = {"name": name}
        if roles:
            payload["roles"] = [{"id": role.id} for role in roles]
        if description:
            payload["description"] = description
        response = requests.post(url, headers=self._headers, json=payload)
        self._raise_error(response)
        return cast(Role, clear_at(response.json()))

    def delete_role(self, id: Union[str, UUID]) -> bool:
        """Delete the role.

        Parameters:
          role: role object.

        Returns:
          bool: True if successful, raise error otherwise.
        """
        url = f"{self.hostname}/api/v3/role/{id}"
        response = requests.delete(url, headers=self._headers)
        self._raise_error(response)
        if response.status_code not in [200, 204]:
            raise DremioError("unknown error", "", response.status_code)
        return True
