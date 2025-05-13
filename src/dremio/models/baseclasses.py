from typing import Optional, Any, Union
from uuid import UUID

from .accesscontrol import AccessControlList, Privilege, Role, SystemRoles, User
from ..utils import to_dict


class DremioObject:
    _dremio: Optional[Any] = None

    def to_dict(self):
        return to_dict(self)


class DremioAccessible(DremioObject):
    id: UUID

    def commit(self):
        """Commit the changes to the Dremio instance. Access role changes will be committed automatically if you don't set `lazy=True` in the methods.
        All other changes have to be committed manually with this method.
        """
        if not self._dremio:
            raise TypeError(
                "This object has no dremio instance!\nTry to commit the dataset directly in the dremio sdk"
            )
        return self._dremio.update_catalog_item(self.id, self)

    def set_access_for_role(self, role: Union[Role, SystemRoles, UUID], privileges: list[Privilege], *, lazy: bool = False):  # type: ignore
        """Add access for a role to the folder."""
        if not self._dremio:
            raise TypeError(
                "This Folder has no dremio instance!\nTry to add access for the role directly in the dremio sdk"
            )
        if self.accessControlList is None:
            self.accessControlList = AccessControlList()
        if role == "PUBLIC":
            role = self._dremio.get_role_by_name("PUBLIC")
        elif role == "ADMIN":
            role = self._dremio.get_role_by_name("ADMIN")

        role: Union[Role, UUID]
        if self.accessControlList.roles is None:
            self.accessControlList.roles = []
        role_id = role.id if isinstance(role, Role) else role
        self.accessControlList.set_access_for_role(role_id, privileges)
        if not lazy:
            self = self.commit()
        return self

    def remove_access_for_role(self, role: Union[Role, SystemRoles, UUID], lazy: bool = False):  # type: ignore
        """Remove access for a role to the folder."""
        if not self._dremio:
            raise TypeError(
                "This Folder has no dremio instance!\nTry to remove access for the role directly in the dremio sdk"
            )
        if self.accessControlList is None:
            return self
        if role == "PUBLIC":
            role = self._dremio.get_role_by_name("PUBLIC")
        elif role == "ADMIN":
            role = self._dremio.get_role_by_name("ADMIN")

        role: Union[Role, UUID]
        if self.accessControlList.roles is None:
            return self
        role_id = role.id if isinstance(role, Role) else role
        self.accessControlList.remove_access_for_role(role_id)
        if not lazy:
            self = self.commit()
        return self

    def set_access_for_user(
        self, user: Union[User, UUID], privileges: list[Privilege], lazy: bool = False
    ):
        """Add access for a user to the folder."""
        if not self._dremio:
            raise TypeError(
                "This Folder has no dremio instance!\nTry to add access for the user directly in the dremio sdk"
            )
        if self.accessControlList is None:
            self.accessControlList = AccessControlList()
        if self.accessControlList.users is None:
            self.accessControlList.users = []
        user_id = user.id if isinstance(user, User) else user
        self.accessControlList.set_access_for_user(user_id, privileges)
        if not lazy:
            self = self.commit()
        return self

    def remove_access_for_user(self, user: Union[User, UUID], lazy: bool = False):
        """Remove access for a user to the folder."""
        if not self._dremio:
            raise TypeError(
                "This Folder has no dremio instance!\nTry to remove access for the user directly in the dremio sdk"
            )
        if self.accessControlList is None:
            return self
        if self.accessControlList.users is None:
            return self
        user_id = user.id if isinstance(user, User) else user
        self.accessControlList.remove_access_for_user(user_id)
        if not lazy:
            self = self.commit()
        return self
