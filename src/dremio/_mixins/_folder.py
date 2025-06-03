__all__ = ["_MixinFolder"]  # this is like `export ...` in typescript
import logging
import re

from dremio.utils.decorators import experimental

from ..utils.converter import path_to_list, path_to_dotted
from ..models import *

from . import BaseClass
from ._catalog import _MixinCatalog
from ._dataset import _MixinDataset


class _MixinFolder(_MixinDataset, _MixinCatalog, BaseClass):
    def get_folder(
        self,
        path: Union[list[str], str, None] = None,
        *,
        id: Union[UUID, str, None] = None,
    ) -> Folder:
        """Get the folder. [learn more](https://github.com/continental/pydremio/blob/master/docs/FOLDER.md)
        \nCould raise a DremioError if the folder does not exist or you have no permissions.

        Parameters:
          path: folder path: "A.B.C" or ["A","B","C"].
          id: folder uuid.

        Returns:
          Folder: folder object.
        """
        data = self._get_catalog_object(id=id, path=path)
        if data["entityType"] != "folder":
            raise TypeError("requested resource is not a folder")
        obj = cast(Folder, data)
        if type(obj) != Folder:
            raise TypeError("requested resource is not a folder")
        obj._dremio = self  # type: ignore
        return obj

    # Holgers implementation with loop
    def create_folder(self, path: Union[str, list[str]]) -> Folder:
        """Create a folder and all parent folders if not exist.

        Parameters:
          path: The path of the folder.
        """
        # create folder(s) (if don't exist)
        if isinstance(path, str):
            path = path_to_list(path)
        target_path_temp = []
        for p in path[:-1]:
            target_path_temp.append(p)
            new_folder = NewFolder(target_path_temp)
            try:
                self.create_catalog_item(new_folder)
            except DremioError as e:
                if e.status_code in [400, 409]:  # folder already exists
                    continue
                raise e
        self.create_catalog_item(NewFolder(path))  # can raise DremioError
        return self.get_folder(path)

    def delete_folder(
        self, path: Union[str, list[str]], recursive: bool = True
    ) -> bool:
        """Delete the folder.
        \nCould raise a TypeError if there is no folder under this path.
        \nCould raise a DremioError if the folder does not exist or you have no permissions.

        Parameters:
          path: The path of the folder.
          recursive: Delete all children. Default: True. If False, it will raise an DremioConnectorError if the folder is not empty.
        """
        if isinstance(path, str):
            path = path_to_list(path)
        folder = self.get_folder(path)
        folder_has_children = len(folder.children) > 0 if folder.children else False
        if not recursive and folder_has_children:
            raise DremioError("Folder not empty", f"Folder at {path} is not empty")
        return self.delete_catalog_item(folder.id)

    def _duplicate_folder(
        self,
        source_path: Union[list[str], str],
        target_path: Union[list[str], str],
        method: Literal["ref", "copy"],
        assume_privileges: bool = True,
        relative_references: bool = False,
        overwrite_existing: bool = False,
        _datasets_to_commit: list[Dataset] = [],
    ) -> Folder:
        source_path = path_to_list(source_path)
        target_path = path_to_list(target_path)
        try:
            source = self.get_folder(source_path)
        except DremioError as e:
            if e.status_code == 404:
                raise DremioError(
                    "Folder not found", f"Folder at {source_path} not found"
                )
            raise e
        if not source:
            raise DremioError("Folder not found", f"Folder at {source_path} not found")
        try:
            target = self.create_folder(target_path)
        except DremioError as e:
            if e.status_code != 409:  # folder already exists
                raise e
            target = self.get_folder(target_path)
        if assume_privileges:  # only on folders
            try:
                target.accessControlList = source.accessControlList
                target.commit()
            except DremioError as e:
                raise DremioError(
                    "Error updating folder",
                    f"Error updating folder {target_path}: {e.errorMessage}",
                )
        for child in source:
            new_child_path = target_path + [child.path[-1]]
            if child.type == "CONTAINER" and child.containerType == "FOLDER":
                if child.path == source_path or child.path == target_path:
                    continue
                self._duplicate_folder(
                    child.path,
                    new_child_path,
                    method,
                    assume_privileges,
                    relative_references,
                    overwrite_existing,
                    _datasets_to_commit,
                )
            elif child.type == "DATASET":
                if method == "copy":
                    ds: Dataset
                    try:
                        ds = self.copy_dataset(
                            child.path,
                            new_child_path,
                            assume_privileges=assume_privileges,
                            overwrite_existing=False,
                        )  # we don't use the existing overwrite flag to implement custom logging
                    except DremioError as e:
                        # warning is needed in multiple cases - made little helper function for it:
                        log_warning = lambda e: logging.warning(
                            f"Failed to copy {path_to_dotted(child.path)} - {e} ({self._get_url_of_object(child.path)})"
                        )

                        if e.status_code == 409:
                            if not overwrite_existing:
                                logging.info(
                                    f"{path_to_dotted(child.path)} already exists and is left as it is"
                                )
                                continue
                            try:
                                logging.info(
                                    f"{path_to_dotted(child.path)} exists and will be overwritten"
                                )
                                self.delete_dataset(new_child_path)
                                ds = self.copy_dataset(
                                    child.path,
                                    new_child_path,
                                    assume_privileges=assume_privileges,
                                    overwrite_existing=False,
                                )
                            except Exception as e:
                                log_warning(e)  # use warning helper lambda
                        else:
                            log_warning(e)  # use warning helper lambda
                            continue
                    # update sql references
                    if relative_references:
                        new_sql = re.sub(
                            r"\"?"
                            + r"\"?\.\"?".join(source_path)
                            + r"\"?",  # find 'Application.TEST_FOLDER' and '"Application"."TEST_FOLDER"'
                            path_to_dotted(target_path),
                            ds.sql or "",
                        )
                        ds.sql = new_sql
                        # add this dataset to a list to commit it later
                        # this is for the case that the datasets, that been used in this SQL statement, are not copied until now.
                        # if we commit after wards, we can face this problem
                        _datasets_to_commit.append(
                            ds
                        )  # has to be committed after wards

                elif method == "ref":
                    try:
                        self.reference_dataset(
                            child.path,
                            new_child_path,
                            assume_privileges=assume_privileges,
                            overwrite_existing=False,
                        )
                    except DremioError as e:
                        log_warning = lambda e: logging.warning(
                            f"Failed to reference {path_to_dotted(child.path)} - {e} ({self._get_url_of_object(child.path)})"
                        )
                        if e.status_code == 409:
                            if not overwrite_existing:
                                logging.info(
                                    f"{path_to_dotted(child.path)} already exists and is left as it is"
                                )
                                continue
                            try:
                                logging.info(
                                    f"{path_to_dotted(child.path)} exists and will be overwritten"
                                )
                                self.delete_dataset(new_child_path)
                                ds = self.reference_dataset(
                                    child.path,
                                    new_child_path,
                                    assume_privileges=assume_privileges,
                                    overwrite_existing=False,
                                )
                            except Exception as e:
                                log_warning(e)
                        else:
                            log_warning(e)
                            continue
        return target

    def copy_folder(
        self,
        source_path: Union[list[str], str],
        target_path: Union[list[str], str],
        *,
        assume_privileges: bool = True,
        relative_references: bool = False,
        overwrite_existing: bool = False,
    ) -> Folder:
        """Copy the folder by path. The copy will point on the same source like the original.
        If the target path does not exist, it will create the folder. [learn more](https://github.com/continental/pydremio/blob/master/docs/DREMIO_METHODS.md#copy-a-folder)

        Parameters:
          source_path: source path of the folder.
          target_path: target path of the folder.
          assume_privileges: Assume privileges. Default: True.
          relative_references: Change the sql statements in datasets to the new path, if they are inside the copied scope -> [learn more](https://github.com/continental/pydremio/blob/master/docs/FOLDER.md#copy-a-folder). Default: False
          overwrite_existing: Overwrites the existing folders and datasets with the same path in the target folder. Default: False

        Returns:
          Folder: The copied folder.
        """
        datasets_to_commit: list[Dataset] = []
        target = self._duplicate_folder(
            source_path,
            target_path,
            "copy",
            assume_privileges,
            relative_references,
            overwrite_existing=overwrite_existing,
            _datasets_to_commit=datasets_to_commit,
        )
        for ds in datasets_to_commit:
            try:
                ds.commit()
            except Exception as e:
                logging.error(
                    f"Error while committing new relative sql statement to {path_to_dotted(ds.path)}.",
                    e,
                )
        return target

    def reference_folder(
        self,
        source_path: Union[list[str], str],
        target_path: Union[list[str], str],
        *,
        assume_privileges: bool = True,
        overwrite_existing: bool = False,
    ) -> Folder:
        """Copy the folder by path. All children will be referenced.
        If the target path does not exist, it will create the folder. [learn more](https://github.com/continental/pydremio/blob/master/docs/FOLDER.md#reference-a-folder)

        Parameters:
          source_path: source path of the folder.
          target_path: target path of the folder.
          assume_privileges: Assume privileges. Default: True.
          overwrite_existing: Overwrites the existing folders and datasets with the same path in the target folder. Default: False

        Returns:
          Folder: The referenced folder.
        """
        return self._duplicate_folder(
            source_path,
            target_path,
            "ref",
            assume_privileges,
            relative_references=False,
            overwrite_existing=overwrite_existing,
        )

    @experimental
    def dump_folder(self, path: list[str] | str, depth:int|None=None) -> dict:
        """⚠️ EXPERIMENTAL: Dump the folder and all children to a dictionary.

        Parameters:
            path (list[str] | str): The path of the folder to dump.
            depth (int | None, optional): The depth of the dump. If None, it will dump all children. Defaults to None.

        Returns:
            dict: The folder and all children as a dictionary.
        """
        folder = self.get_folder(path)
        folder_dict = folder.to_dict()

        folder_dict["children"] = []  # clear children to avoid recursion 
        for child in folder:
            if child.type == "CONTAINER" and child.containerType == "FOLDER" and (depth is None or depth > 0):
                child = self.dump_folder(child.path, depth=depth-1 if depth is not None else None)
            elif child.type == "DATASET":
                ds = self.get_dataset(child.path)
                child = ds.to_dict()
            else:
                logging.warning(f"Child {child.path} is not a folder or dataset, skipping it.")
                continue
            folder_dict['children'].append(child)
        return folder_dict
