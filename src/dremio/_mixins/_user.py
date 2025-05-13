__all__ = ["_MixinUser"]  # this is like `export ...` in typescript
from typing import Any
import requests
from dataclasses import asdict

from ..utils.converter import clear_at
from ..models import *

from . import BaseClass
from ._catalog import _MixinCatalog


class _MixinUser(_MixinCatalog, BaseClass):

    def get_users(self) -> list[User]:
        """Get the users.

        Returns:
          list[User]: The users.
        """
        url = f"{self.hostname}/api/v3/user"
        response = requests.get(url, headers=self._headers)
        self._raise_error(response)
        return [cast(User, clear_at(user)) for user in response.json()]

    def get_user(self, id: Union[UUID, str]) -> User:
        """Get the user.

        Parameters:
          id: user uuid.

        Returns:
          User: user.
        """
        url = f"{self.hostname}/api/v3/user/{id}"
        response = requests.get(url, headers=self._headers)
        self._raise_error(response)
        return cast(User, clear_at(response.json()))

    def get_user_by_name(self, name: str) -> User:
        """Get the user by name.

        Parameters:
          name: users name.

        Returns:
          User: user.
        """
        url = f"{self.hostname}/api/v3/user/by-name/{name}"
        response = requests.get(url, headers=self._headers)
        self._raise_error(response)
        return cast(User, clear_at(response.json()))

    def create_user(self, user: User) -> User:
        """Create the user.

        Parameters:
          user: new user object.

        Returns:
          User: user object from dremio after creation.
        """
        url = f"{self.hostname}/api/v3/user"
        payload: dict[str, Any] = asdict(user, dict_factory=User.dict_factory)
        response = requests.post(url, headers=self._headers, json=payload)
        self._raise_error(response)
        return cast(User, clear_at(response.json()))

    def update_user(self, id: Union[UUID, str], user: User) -> User:
        """Update the user.

        Parameters:
          id: user uuid.
          user: user object with changes.

        Returns:
          User: user from dremio after changes.
        """
        url = f"{self.hostname}/api/v3/user/{id}"
        payload: dict[str, Any] = asdict(user, dict_factory=User.dict_factory)
        response = requests.put(url, headers=self._headers, json=payload)
        self._raise_error(response)
        return cast(User, clear_at(response.json()))

    def delete_user(self, id: Union[UUID, str], tag: str) -> bool:
        """Delete the user.

          Parameters:
            id: user uuid.
          tag: version tag of current user version.

        Returns:
          bool: True if successful, False otherwise.
        """
        url = f"{self.hostname}/api/v3/user/{id}?version={tag}"
        response = requests.delete(url, headers=self._headers)
        self._raise_error(response)
        if response.status_code not in [200, 204]:
            raise DremioError("unknown error", "", response.status_code)
        return True
