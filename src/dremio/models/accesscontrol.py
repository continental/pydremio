from dataclasses import dataclass
from typing import Literal, Optional
from uuid import UUID
from enum import Enum


Privilege = Literal[
    "READ",
    "WRITE",
    "ALTER_REFLECTION",
    "SELECT",
    "ALTER",
    "VIEW_REFLECTION",
    "MODIFY",
    "MANAGE_GRANTS",
    "CREATE_TABLE",
    "DROP",
    "EXTERNAL_QUERY",
    "INSERT",
    "TRUNCATE",
    "DELETE",
    "UPDATE",
    "EXECUTE",
    "CREATE_SOURCE",
    "ALL",
]
SystemRoles = Literal["ADMIN", "PUBLIC"]


@dataclass
class AccessControl:
    id: UUID
    permissions: list[Privilege]


@dataclass
class AccessControlList:
    users: Optional[list[AccessControl]] = None
    roles: Optional[list[AccessControl]] = None

    def __post_init__(self):
        if self.users is None:
            self.users = []
        if self.roles is None:
            self.roles = []

    def set_access_for_user(self, user_id: UUID, privileges: list[Privilege]):
        if self.users is None:
            self.users = []
        for user in self.users:
            if user.id == user_id:
                user.permissions = privileges
                return
        self.users.append(AccessControl(id=user_id, permissions=privileges))

    def add_access_for_user(self, user_id: UUID, privileges: list[Privilege]):
        if self.users is None:
            self.users = []
        for user in self.users:
            if user.id == user_id:
                user.permissions += privileges
                return
        self.users.append(AccessControl(id=user_id, permissions=privileges))

    def remove_access_for_user(self, user_id: UUID):
        if self.users is None:
            self.users = []
        for user in self.users:
            if user.id == user_id:
                self.users.remove(user)
                return

    def set_access_for_role(self, role_id: UUID, privileges: list[Privilege]):
        if self.roles is None:
            self.roles = []
        for role in self.roles:
            if role.id == role_id:
                role.permissions = privileges
                return
        self.roles.append(AccessControl(id=role_id, permissions=privileges))

    def add_access_for_role(self, role_id: UUID, privileges: list[Privilege]):
        if self.roles is None:
            self.roles = []
        for role in self.roles:
            if role.id == role_id:
                role.permissions += privileges
                return
        self.roles.append(AccessControl(id=role_id, permissions=privileges))

    def remove_access_for_role(self, role_id: UUID):
        if self.roles is None:
            self.roles = []
        for role in self.roles:
            if role.id == role_id:
                self.roles.remove(role)
                return


@dataclass
class Owner:
    ownerId: UUID
    ownerType: str  # Literal['USER']


@dataclass
class Permissions:
    canUploadProfiles: bool
    canDownloadProfiles: bool
    canEmailForSupport: bool
    canChatForSupport: bool
    canViewAllJobs: bool
    canCreateUser: bool
    canCreateRole: bool
    canCreateSource: bool
    canUploadFile: bool
    canManageNodeActivity: bool
    canManageEngines: bool
    canManageQueues: bool
    canManageEngineRouting: bool
    canManageSupportSettings: bool


@dataclass
class Role:
    id: UUID
    name: str
    type: Literal["INTERNAL", "EXTERNAL", "SYSTEM"]
    roles: Optional[list[dict[str, str]]] = None
    memberCount: Optional[int] = None
    description: Optional[str] = None


# TODO: Add member control


@dataclass
class User:
    id: UUID
    name: str
    firstName: str
    lastName: str
    email: str
    tag: str
    active: bool
    type: Optional[Literal["EnterpriseUser", "User"]] = None
    roles: Optional[list[Role]] = None
    source: Optional[str] = None

    @staticmethod
    def dict_factory(x):
        change = {"type": "@type"}
        return {k: v if k not in change else change[k] for (k, v) in x}


@dataclass
class CurrentUser:
    token: str
    userName: str
    firstName: str
    lastName: str
    expires: int
    email: str
    userId: str
    admin: bool
    clusterId: str
    clusterCreatedAt: int
    version: str
    permissions: Permissions
    userCreatedAt: int
