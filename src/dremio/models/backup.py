from dataclasses import dataclass
import json
from .. import dremio
from typing import Any, Literal, Optional, Union
from .accesscontrol import Role, User
from ..utils.converter import clear_at, to_dict


@dataclass
class Dependency:
    needed_by: list[str]  # list of IDs
    resource: dict  # dataset

    def as_dict(self) -> dict:
        return to_dict(self)

    def json(self) -> str:
        return json.dumps(self.as_dict(), indent=2)


@dataclass
class Backup:
    tree: dict[str, Any]
    users: list[User]
    roles: list[Role]
    dependencies: dict[str, Dependency]

    @staticmethod
    def load(file_path: str) -> "Backup":
        ext = file_path.split(".")[-1]
        if ext == "json":
            with open(file_path, "r") as f:
                data = json.load(f)
        else:
            raise ValueError("Invalid file extension")
        return Backup(data=data)

    def __init__(
        self,
        tree: dict[str, Any] = {},
        users: list[User] = [],
        roles: list[Role] = [],
        dependencies: dict[str, Dependency] = {},
        *,
        data: Union[dict, Any] = None,
    ):
        # set default
        self.tree = {}
        self.users = []
        self.roles = []
        self.dependencies = {}

        # parse data as Backup or dict
        if data:
            if isinstance(data, dict):
                self._backup_from_dict(data)
            elif isinstance(data, Backup):
                self.tree = data.tree
                self.users = data.users
                self.roles = data.roles
                self.dependencies = data.dependencies
            else:
                raise TypeError("restore data arg is not a valid type")

        # make it possible to overwrite attrs
        if tree:
            self.tree = tree
        if users:
            self.users = users
        if roles:
            self.roles = roles
        if dependencies:
            self.dependencies = dependencies

    def _backup_from_dict(self, data: dict):
        if "users" not in data:
            data["users"] = []
        if "roles" not in data:
            data["roles"] = []
        try:
            self.tree = data["tree"]
            self.users = [User(**clear_at(user)) for user in data["users"]]
            self.roles = [Role(**role) for role in data["roles"]]
            self.dependencies = {}
            try:
                for path, dep in data["dependencies"].items():
                    try:
                        self.dependencies[path] = Dependency(
                            needed_by=dep["needed_by"], resource=dep["resource"]
                        )
                    except:
                        print(f" dep '{path}' not loadable")
                        continue
            except:
                print("dependencies can not be loaded")

        except:
            raise TypeError("Invalid data")

    def as_dict(self) -> dict:
        return to_dict(self)

    def json(self) -> str:
        return json.dumps(self.as_dict(), indent=2)

    def save(self, path: str):
        ext = path.split(".")[-1]
        if ext == "json":
            with open(path, "w") as f:
                f.write(self.json())
        else:
            raise ValueError("Invalid file extension")

    def restore(
        self, dremio: Any, target_path: Union[str, None] = None, retry_limit: int = 5
    ) -> "RestoreReport":
        if not target_path:
            return dremio.restore(self, retry_limit)
        else:
            return dremio.restore_on_path(self, target_path, retry_limit)


@dataclass
class RestoreReportObject:
    path: list[str]
    old_id: str
    new_id: str
    old_path: Optional[list[str]] = None


ReasonToFail = Literal["permission", "unknown"]


@dataclass
class Failed:
    reason: ReasonToFail
    data: RestoreReportObject


@dataclass
class RestoreReport:
    missing_dependencies: dict[str, list[str]]  # {node_path:[dep_path]}
    successful: list[RestoreReportObject]
    already_exists: list[RestoreReportObject]
    missing_users: list[User]
    failed: list[Failed]

    def __init__(self):
        self.missing_dependencies = {}
        self.successful = []
        self.already_exists = []
        self.missing_users = []
        self.failed = []

    def add_missing_dependency(self, node_path: str, dependency_path: str):
        if node_path in self.missing_dependencies:
            self.missing_dependencies[node_path].append(dependency_path)
        else:
            self.missing_dependencies[node_path] = []

    def add_successful(
        self,
        path: list[str],
        new_id: str,
        old_id: str = "",
        old_path: Union[list[str], None] = None,
    ):
        r = RestoreReportObject(
            path=path, old_id=old_id, new_id=new_id, old_path=old_path
        )
        self.successful.append(r)

    def add_already_exists(
        self, path: list[str], old_id: str, old_path: Union[list[str], None] = None
    ):
        r = RestoreReportObject(path=path, old_id=old_id, new_id="", old_path=old_path)
        self.already_exists.append(r)

    def add_failed(
        self,
        reason: ReasonToFail,
        path: list[str],
        old_id: str,
        old_path: Union[list[str], None] = None,
    ):
        r = RestoreReportObject(path=path, old_id=old_id, new_id="", old_path=old_path)
        self.failed.append(Failed(reason, r))

    def as_dict(self) -> dict:
        return to_dict(self)

    def json(self) -> str:
        return json.dumps(self.as_dict(), indent=2)

    def save(self, path: str):
        ext = path.split(".")[-1]
        if ext == "json":
            with open(path, "w") as f:
                f.write(self.json())
        else:
            raise ValueError("Invalid file extension")
