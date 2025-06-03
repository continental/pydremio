from dataclasses import dataclass
from uuid import UUID
from typing import Optional, Literal, Union
from typing_extensions import Self

from dremio.utils.decorators import experimental

from ..exceptions import DremioConnectorError

from .custom_objects import CatalogElement

from .accesscontrol import (
    Owner,
    AccessControlList,
    Privilege,
)


from .baseclasses import DremioAccessible, DremioObject
from ..utils.converter import path_to_dotted


@dataclass
class Folder(DremioAccessible, DremioObject):
    entityType: Literal["folder"]
    id: UUID
    path: list[str]
    tag: str
    children: Optional[list[CatalogElement]] = None
    accessControlList: Optional[AccessControlList] = None
    permissions: Optional[list[Privilege]] = None
    owner: Optional[Owner] = None

    def __iter__(self):
        if not self.children:
            return iter([])
        return iter(self.children)

    def __next__(self):
        if not self.children:
            return iter([])
        for child in self.children:
            yield child

    def __getitem__(self, key: Union[int, slice]) -> list[CatalogElement]:
        if not self.children:
            raise IndexError("This folder has no children")
        if isinstance(key, slice):
            return self.children[key.start : key.stop : key.step]
        else:
            raise IndexError("Key must be slice. For example folder[1:3]")

    def __str__(self) -> str:
        return f"Folder(path = '{path_to_dotted(self.path)}', id = {self.id})"

    def __repr__(self) -> str:
        return f"Folder(path = '{path_to_dotted(self.path)}', id = {self.id})"

    @property
    def name(self) -> str:
        return self.path[-1]

    def pull(self) -> Self:
        """Get the newest version of this folder form dremio.
        Could be useful for multi-threading and updated children.
        This is the opposite of .commit()

        Raises:
            TypeError: raises if no dremio instance is connected

        Returns:
            Self: gives you itself for chaining
        """
        if not self._dremio:
            raise TypeError(
                "This object has no dremio instance!\nTry to commit the dataset directly in the dremio sdk"
            )
        self = self._dremio.get_folder(self.id)
        return self

    def copy(
        self,
        target_path: Union[list[str], str],
        *,
        assume_privileges: bool = True,
        relative_references: bool = False,
        overwrite_existing: bool = False,
    ) -> "Folder":
        """Copy the folder to the target path. Same as `Dremio.copy_folder(this_folder_path, target_path)`. [learn more](https://github.com/continental/pydremio/blob/master/docs/FOLDER.md#copy-a-folder)"""
        if not self._dremio:
            raise TypeError(
                "This Folder has no dremio instance!\nTry to copy the folder directly in the dremio sdk"
            )
        return self._dremio.copy_folder(
            self.path,
            target_path,
            assume_privileges=assume_privileges,
            relative_references=relative_references,
            overwrite_existing=overwrite_existing,
        )

    def reference(
        self,
        target_path: Union[list[str], str],
        *,
        assume_privileges: bool = True,
        overwrite_existing: bool = False,
    ) -> "Folder":
        """Reference the folder to the target path. Same as `Dremio.reference_folder(this_folder_path, target_path)`. [learn more](https://github.com/continental/pydremio/blob/master/docs/FOLDER.md#reference-a-folder)"""
        if not self._dremio:
            raise TypeError(
                "This Folder has no dremio instance!\nTry to reference the folder directly in the dremio sdk"
            )
        return self._dremio.reference_folder(
            self.path,
            target_path,
            assume_privileges=assume_privileges,
            overwrite_existing=overwrite_existing,
        )

    def delete(self, recursive=False) -> None:
        """Delete the folder. Same as `Dremio.delete_folder(this_folder_path)`."""
        if not self._dremio:
            raise TypeError(
                "This Folder has no dremio instance!\nTry to delete the folder directly in the dremio sdk"
            )
        success: bool = False
        try:
            success = self._dremio.delete_folder(self.path, recursive=recursive)
        except Exception as e:
            raise e
        if not success:
            raise DremioConnectorError(
                "Folder not deleted", f"Folder {self.path} could not be deleted"
            )
        del self

    @experimental
    def dump(self, depth: int|None = None) -> dict:
        """⚠️ EXPERIMENTAL: Dump the folder with all children as a dictionary. Same as `Dremio.dump_folder(this_folder_path, depth=depth)`.

        Parameters:
            depth (int|None, optional): The depth of the dump. If None, it will dump all children. Defaults to None.

        Raises:
            TypeError: raises if no dremio instance is connected

        Returns:
            dict: the dumped folder as a dictionary
        """
        if not self._dremio:
            raise TypeError(
                "This Folder has no dremio instance!\nTry to dump the folder directly in the dremio sdk"
            )
        return self._dremio.dump_folder(self.path, depth=depth)

