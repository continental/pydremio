__all__ = ["_MixinDataset"]  # this is like `export ...` in typescript

import logging
import requests

from ..utils.converter import to_dict, path_to_list, path_to_dotted
from ..models import *

from . import BaseClass
from ._catalog import _MixinCatalog
from ._sql import _MixinSQL
from ._reflection import _MixinReflection


class _MixinDataset(_MixinReflection, _MixinSQL, _MixinCatalog, BaseClass):

    def get_dataset(
        self,
        path: Union[list[str], str, None] = None,
        *,
        id: Union[UUID, str, None] = None,
    ) -> Dataset:
        """Get the dataset. [learn more](https://github.com/continental/pydremio/blob/master/docs/DATASET.md)
        \nCould raise a DremioError if the dataset does not exist or you have no permissions.

        Parameters:
          path: dataset path: "A.B.C" or ["A","B","C"].
          id: dataset uuid.

        Returns:
          Dataset: dataset object.
        """
        obj = cast(Dataset, self._get_catalog_object(id=id, path=path))
        if type(obj) != Dataset:
            raise TypeError("requested resource is not a dataset")
        obj._dremio = self  # type: ignore
        return obj

    def create_dataset(
        self,
        path: Union[list[str], str],
        sql: Union[str, SQLRequest],
        type: Literal["PHYSICAL_DATASET", "VIRTUAL_DATASET"] = "VIRTUAL_DATASET",
    ) -> Dataset:
        """Create a new dataset. [learn more](https://github.com/continental/pydremio/blob/master/docs/DATASET.md)
        \nShortcut for `Dremio.create_catalog_item(NewDataset)` with type VIRTUAL_DATASET.

        Parameters:
          path: dataset path: "A.B.C" or ["A","B","C"].
          sql: sql request as string or SQLRequest object, if sql context is needed.
          type: dataset type. Default: VIRTUAL_DATASET.

        Returns:
          Dataset: dataset object.
        """
        if isinstance(sql, str):
            sql = SQLRequest(sql)
        ds = NewDataset(path_to_list(path), type, sql.sql)
        new_ds = self.create_catalog_item(ds)  #! could raise DremioError
        return self.get_dataset(id=new_ds.id)

    def delete_dataset(self, path: Union[list[str], str]) -> bool:
        """Delete the dataset. [learn more](https://github.com/continental/pydremio/blob/master/docs/DATASET.md)
        \nCould raise a TypeError if there is no dataset under this path.
        \nCould raise a DremioError if the dataset does not exist or you have no permissions.

        Parameters:
          path: dataset path: "A.B.C" or ["A","B","C"].
        """
        if isinstance(path, str):
            path = path_to_list(path)
        dataset = self.get_dataset(path)
        return self.delete_catalog_item(dataset.id)

    def get_lineage(self, id: Union[UUID, str]) -> Lineage:
        """Get the lineage."""
        url = f"{self.hostname}/api/v3/catalog/{str(id)}/graph"
        response = requests.get(url, headers=self._headers)
        self._raise_error(response)
        return cast(Lineage, response.json())

    def get_wiki(self, id: Union[UUID, str]) -> Union[Wiki, None]:
        """Get the wiki."""
        url = f"{self.hostname}/api/v3/catalog/{str(id)}/collaboration/wiki"
        response = requests.get(url, headers=self._headers)
        if self._stand_alone_404(response):
            return
        self._raise_error(response)
        obj = cast(Wiki, response.json())
        if type(obj) != Wiki:
            raise TypeError("requested resource is not a wiki")
        obj._dataset_id = id if isinstance(id, UUID) else UUID(id)
        obj._dremio = self
        return obj

    def set_wiki(
        self, id: Union[UUID, str], wiki: Wiki, auto_versioning: bool = False
    ) -> Wiki:
        """Set the wiki."""
        url = f"{self.hostname}/api/v3/catalog/{str(id)}/collaboration/wiki"
        if auto_versioning:
            old_wiki = self.get_wiki(id)
            wiki.version = old_wiki.version if old_wiki else 0
        payload = {"text": wiki.text, "version": wiki.version}
        response = requests.post(url, json=payload, headers=self._headers)
        self._raise_error(response)
        return cast(Wiki, response.json())

    def get_tags(self, id: str) -> Union[Tags, None]:
        """Get the tags."""
        url = f"{self.hostname}/api/v3/catalog/{id}/collaboration/tag"
        response = requests.get(url, headers=self._headers)
        if self._stand_alone_404(response):
            return
        self._raise_error(response)
        return cast(Tags, response.json())

    def set_tags(self, id: str, tags: Tags) -> Tags:
        """Set the tags."""
        url = f"{self.hostname}/api/v3/catalog/{id}/collaboration/tag"
        payload = to_dict(tags)
        response = requests.post(url, json=payload, headers=self._headers)
        self._raise_error(response)
        return cast(Tags, response.json())

    def copy_dataset(
        self,
        source_path: Union[list[str], str],
        target_path: Union[list[str], str],
        *,
        assume_privileges: bool = True,
        overwrite_existing: bool = False,
    ) -> Dataset:
        """Copy the dataset by path. The copy will point on the same source like the original.
        If the target path is already exists and overwrite flag is False, it will course an DremioError.

        Parameters:
          source_path: source location path of the dataset.
          target_path: target location path of the dataset.
          assume_privileges: Assume privileges. Default: True.
          overwrite_existing: Overwrites the existing folders and datasets with the same path in the target folder. Default: False


        Returns:
          Dataset: The copied dataset.
        """
        source_ds = self.get_dataset(source_path)
        if type(source_ds) != Dataset:
            raise ValueError("requested resource is not a dataset")
        source_ds.path = path_to_list(target_path)
        try:
            self.create_catalog_item(source_ds)
        except DremioError as e:
            if e.status_code == 409:
                if not overwrite_existing:
                    raise DremioError(
                        "Conflict", f"Item at {target_path} already exists", 409
                    )
                try:
                    logging.info(
                        f"{path_to_dotted(target_path)} exists and is been overwritten"
                    )
                    self.delete_dataset(target_path)
                    self.copy_dataset(
                        source_path,
                        target_path,
                        assume_privileges=assume_privileges,
                        overwrite_existing=overwrite_existing,
                    )
                except Exception as e:
                    logging.error(
                        f"Error while overwriting {path_to_dotted(target_path)}.", e
                    )
                    raise e
            else:
                raise e
        target_ds = self.get_dataset(target_path)
        if not assume_privileges and target_ds.accessControlList:
            target_ds.accessControlList.users = []
            target_ds.accessControlList.roles = []
            target_ds.commit()
            target_ds = self.get_dataset(target_ds.path)
        return target_ds

    def reference_dataset(
        self,
        source_path: Union[list[str], str],
        target_path: Union[list[str], str],
        *,
        assume_privileges: bool = True,
        overwrite_existing: bool = False,
    ) -> Dataset:
        """Reference the dataset by path. The reference will point on the original.
        If the target path is already exists and overwrite flag is False, it will course an DremioError.

        Parameters:
          source_path: source location path of the dataset.
          target_path: target location path of the dataset.
          assume_privileges: Assume privileges. Default: True.
          overwrite_existing: Overwrites the existing folders and datasets with the same path in the target folder. Default: False

        Returns:
          Dataset: The referenced dataset.
        """
        # check if there already is a dataset on the target path
        try:
            already_existing_ds = self.get_dataset(target_path)
            if not overwrite_existing:
                raise DremioError(
                    "Conflict", f"Item at {target_path} already exists", 409
                )
            already_existing_ds.delete()
        except:
            pass
        source_ds = self.get_dataset(source_path)
        reference_statement = f"SELECT * FROM {path_to_dotted(source_path)}"
        q = SQLRequest(
            f"CREATE VDS {path_to_dotted(target_path)} AS {reference_statement}"
        )
        _, job, e = self._http_query_result(q, return_exception=True)
        if not job:
            raise DremioError("Job not started", f"Error: {e}")
        if job.jobState in ["CANCELED", "FAILED"]:
            raise DremioError(
                job.errorMessage,
                f"VDS from {source_path} at {target_path} can't be created. Message: {job.cancellationReason}",
            )
        if job.jobState != "COMPLETED":
            raise DremioError(
                "Job not completed",
                f"VDS from {source_path} at {target_path} can't be created. Message: {e}",
            )
        target_ds = self.get_dataset(target_path)
        if assume_privileges:
            target_ds.accessControlList = source_ds.accessControlList
            target_ds.commit()
            target_ds = self.get_dataset(target_ds.path)
        return target_ds

    def get_reflections_from_dataset(
        self,
        path: Union[list[str], str, None] = None,
        *,
        id: Union[UUID, str, None] = None,
    ) -> list[Reflection]:
        """Loads all reflections of the given dataset.

        Args:
            path (Union[list[str], str, None], optional): Path to dataset.
            id (Union[UUID, str, None], optional): ID of dataset. Defaults to None.

        Returns:
            list[Reflection]: List of all reflections of this dataset. Empty list if there is none.
        """
        ds = self.get_dataset(path=path, id=id)
        url = f"{self.hostname}/api/v3/dataset/{ds.id}/reflection"
        response = requests.get(url, headers=self._headers)
        self._raise_error(response)
        try:
            data = response.json()["data"]
        except KeyError:
            raise KeyError("Expected data-tag in reflections response, but not found")
        return [cast(Reflection, reflection) for reflection in data]

    def recommended_reflections(
        self, dataset_id: str | UUID, type: Literal["ALL", "RAW", "AGG"] = "ALL"
    ) -> list[NewReflection]:
        """Retrieving reflection recommendations for a Dataset.

        Args:
            dataset_id (str|UUID): Id of dataset to reflect.
            type (Literal[&quot;ALL&quot;, &quot;RAW&quot;, &quot;AGG&quot;], optional): The type of reflection recommendations you want to create and retrieve. Defaults to "ALL".

        Returns:
            Reflections (list[NewReflection]): List of recommended reflections.
        """
        url = f"{self.hostname}/api/v3/dataset/{dataset_id}/reflection/recommendation/{type}/"
        response = requests.post(url, headers=self._headers)
        self._raise_error(response)
        try:
            data = response.json()["data"]
        except KeyError:
            raise KeyError("Expected data-tag in reflections response, but not found")
        return [cast(NewReflection, reflection) for reflection in data]

    # This is here bc it needs `get_dataset`
    def create_recommended_reflections(
        self, dataset_id: str | UUID, type: Literal["ALL", "RAW", "AGG"] = "ALL"
    ) -> list[Reflection]:
        """Creating and Retrieving Reflection Recommendations for a Dataset.

        Args:
            dataset_id (str|UUID): Id of dataset to reflect.
            type (Literal[&quot;ALL&quot;, &quot;RAW&quot;, &quot;AGG&quot;], optional): The type of reflection recommendations you want to create and retrieve. Defaults to "ALL".

        Returns:
            Reflections (list[Reflection]): List of all reflections of the dataset.
        """
        ds = self.get_dataset(id=dataset_id)
        recommanded_reflections = self.recommended_reflections(
            dataset_id=dataset_id, type=type
        )
        refs: list[Reflection] = []
        for ref in recommanded_reflections:
            name = f"{ds.path[-1]}_{ref.type}_{str(ds.id)[:4]}"
            r = self.create_reflection(dataset_id=dataset_id, name=name, reflection=ref)
            refs.append(r)
        return refs

    def refresh_dataset(self, path: Union[list[str], str]) -> None:
        """Refresh dataset reflection"""
        ds = self.get_dataset(path)
        self._refresh_catalog(ds.id)

    def refresh_dataset_metadata(self, path: Union[list[str], str]):
        """Refresh metadata of physical datasets.

        Args:
            path (Union[list[str], str]): Path to physical dataset.
        """
        sql = f"ALTER TABLE {path_to_dotted(path)} REFRESH METADATA;"
        self._http_query_result(sql)
