from dataclasses import InitVar, dataclass, field
from optparse import Option
import logging
from uuid import UUID
from typing import Any, Iterable, Optional, Literal, Type, Union, List, cast
from typing_extensions import Self
from datetime import datetime
from time import sleep
import pyarrow as pa
import pandas as pd
import polars as pl

from .schema import SQLType, Schema, SchemaField

from ..models.reflections import NewReflection, Reflection

from ..exceptions import DremioConnectorError

from .custom_objects import DatasetFormat
from .dremio_utils import Wiki

from .accesscontrol import (
    Owner,
    AccessControlList,
)

from .jobs import JobResult
from .dremio_utils import DatasetAccelerationRefreshPolicy

from .baseclasses import DremioAccessible, DremioObject
from ..utils.converter import path_to_dotted


@dataclass
class TypeClass:
    name: str
    precision: Optional[int] = None
    scale: Optional[int] = None


@dataclass
class Field:
    name: str
    type: TypeClass

    def __str__(self) -> str:
        return f"{self.name} ({self.type.name})"


class Columns:
    __fields: list[Field]

    def __init__(self, fields: list[Field]):
        self.__fields = fields

    def __getattr__(self, name: str) -> str:
        for f in self.__fields:
            if f.name == name:
                return f.name
        raise AttributeError(f"{name} is not an attribute of Fields")

    def __getitem__(self, key: str) -> Field:
        for f in self.__fields:
            if f.name == key:
                return f
        raise KeyError(f"{key} is not in Fields")

    def __str__(self) -> str:
        return "\t".join([str(f) for f in self.__fields])


def cols_type(**fields) -> Type[Columns]:
    return cast(Type[Columns], type("DatasetColumns", (Columns,), fields))


@dataclass
class Dataset(DremioAccessible, DremioObject):
    """The Dremio dataset object is a representation of a dataset in Dremio and contains all needed meta data.
    To use the data behind this dataset run it with `.run()`.

    It's necessary to commit the dataset after changing any values.

    ```python
    dataset = dremio.get_dataset('a.b.c')
    dataset.sql = "SELECT * FROM a.b.d"
    dataset.commit()
    ```

    """

    entityType: Literal["dataset"]
    id: UUID
    type: Literal["PHYSICAL_DATASET", "VIRTUAL_DATASET"]
    path: list[str]
    createdAt: datetime
    tag: str
    fields: list[Field]
    sql: Optional[str] = None
    sqlContext: Optional[list[str]] = None
    owner: Optional[Owner] = None
    accelerationRefreshPolicy: Optional[DatasetAccelerationRefreshPolicy] = None
    format: Optional[DatasetFormat] = None
    approximateStatisticsAllowed: Optional[bool] = None
    accessControlList: Optional[AccessControlList] = None
    _col: InitVar[Columns] = Columns([])

    def __post_init__(self, col: Columns):
        DatasetColumns = cols_type(**{f.name: f.name for f in self.fields})
        self._col = DatasetColumns(self.fields)  # type: ignore

    @property
    def col(self):
        return self._col  # type: ignore

    def __str__(self) -> str:
        return f"Dataset(path = '{path_to_dotted(self.path)}', id = {self.id})"

    def __repr__(self) -> str:
        return f"Dataset(path = '{path_to_dotted(self.path)}', id = {self.id})"

    @property
    def name(self) -> str:
        return self.path[-1]

    def pull(self) -> Self:
        """Get the newest version of this dataset from dremio.
        Could be useful for multi-threading.
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
        self = self._dremio.get_dataset(id=self.id)
        return self

    def run(self, method: Literal["arrow", "http"] = "arrow") -> JobResult:
        """Run a job on the dataset and get the results. [learn more](https://github.com/continental/pydremio/blob/master/docs/DATASET.md#run)
        Per default it uses arrow flight to get the data.
        http is slower and should only be used if arrow is not available.
        You can run `json()`, `to_polars()` or `to_pandas()` on the result to use the data.

        Parameters:
          method (str): The method to run the job. Can be "arrow" or "http". Default is "arrow".

        Returns:
          JobResult: Result of the dataset object.
        """
        if method == "http":
            if not self._dremio:
                raise TypeError(
                    "This Dataset has no dremio instance!\nTry to run the job directly in the dremio sdk"
                )
            job_id = self._dremio._http_start_job_on_dataset(self.id).id
            while True:
                job = self._dremio.get_job_info(job_id)
                state = job.jobState
                if state == "COMPLETED":
                    break
                elif state == "FAILED":
                    raise DremioConnectorError(
                        f"Dataset {self.id} run failed...", f"{job}"
                    )
                elif state == "CANCELED":
                    raise DremioConnectorError(
                        f"Dataset {self.id} run canceled...",
                        f"{job.cancellationReason} {job}",
                    )
                else:
                    sleep(0.5)
            return self._dremio._http_get_job_result(job_id)
        elif method == "arrow":
            if not self._dremio:
                raise TypeError(
                    "This Dataset has no dremio instance!\nTry to run the job directly in the dremio sdk"
                )
            table: pa.Table = self._dremio._flight_query(
                f"SELECT * FROM {path_to_dotted(self.path)}"
            )
            return JobResult.from_arrow_table(table)
        else:
            raise ValueError("method must be 'arrow' or 'http'")

    def run_to_pandas(self) -> pd.DataFrame:
        """Get the results via arrow flight as pandas dataframe. [learn more](https://github.com/continental/pydremio/blob/master/docs/DATASET.md#run)
        Shortcut for `Dataset.run().to_pandas()`

        Returns:
          pd.DataFrame: Result of the dataset object as pandas dataframe.
        """
        if not self._dremio:
            raise TypeError(
                "This Dataset has no dremio instance!\nTry to run the job directly in the dremio sdk"
            )
        table: pa.Table = self._dremio._flight_query(
            f"SELECT * FROM {path_to_dotted(self.path)}"
        )
        return table.to_pandas()

    def run_to_polars(self) -> pl.DataFrame:
        """Get the results via arrow flight as polars dataframe. [learn more](https://github.com/continental/pydremio/blob/master/docs/DATASET.md#run)
        Shortcut for `Dataset.run().to_polars()`

        Returns:
          pl.DataFrame: Result of the dataset object as polars dataframe.
        """
        if not self._dremio:
            raise TypeError(
                "This Dataset has no dremio instance!\nTry to run the job directly in the dremio sdk"
            )
        table: pa.Table = self._dremio._flight_query(self.sql)
        return pl.DataFrame(table)

    def copy(
        self,
        target_path: Union[list[str], str],
        *,
        assume_privileges: bool = True,
        overwrite_existing: bool = False,
    ) -> "Dataset":
        """Copy the dataset to the target path. Same as `Dremio.copy_dataset(this_dataset_path, target_path)`"""
        if not self._dremio:
            raise TypeError(
                "This Dataset has no dremio instance!\nTry to copy the dataset directly in the dremio sdk"
            )
        return self._dremio.copy_dataset(
            self.path,
            target_path,
            assume_privileges=assume_privileges,
            overwrite_existing=overwrite_existing,
        )

    def reference(
        self,
        target_path: Union[list[str], str],
        *,
        assume_privileges: bool = True,
        overwrite_existing: bool = False,
    ) -> "Dataset":
        """Reference the dataset to the target path. Same as `Dremio.reference_dataset(this_dataset_path, target_path)`"""
        if not self._dremio:
            raise TypeError(
                "This Dataset has no dremio instance!\nTry to reference the dataset directly in the dremio sdk"
            )
        return self._dremio.reference_dataset(
            self.path,
            target_path,
            assume_privileges=assume_privileges,
            overwrite_existing=overwrite_existing,
        )

    def create_reflection(self, name: str, reflection: NewReflection) -> Reflection:
        """Create reflection for a Dataset.

        Args:
            name (str): Name for reflection.
            reflection (NewReflection): Reflections settings

        Returns:
            Reflection (Reflection): New reflection.
        """
        if not self._dremio:
            raise TypeError(
                "This Dataset has no dremio instance!\nTry to refresh the dataset directly in the dremio sdk"
            )
        return self._dremio.create_reflection(
            dataset_id=self.id, name=name, reflection=reflection
        )

    def create_recommended_reflections(
        self, type: Literal["ALL", "RAW", "AGG"] = "ALL"
    ) -> list[Reflection]:
        """Creating and Retrieving Reflection Recommendations for a Dataset.

        Args:
            type (Literal[&quot;ALL&quot;, &quot;RAW&quot;, &quot;AGG&quot;], optional): The type of reflection recommendations you want to create and retrieve. Defaults to "ALL".

        Returns:
            Reflections (list[Reflection]): List of all reflections of the dataset.
        """
        if not self._dremio:
            raise TypeError(
                "This Dataset has no dremio instance!\nTry to refresh the dataset directly in the dremio sdk"
            )
        return self._dremio.create_recommended_reflections(self.id, type)

    def get_reflections(self) -> list[Reflection]:
        """Returns all reflections of this dataset.

        Raises:
            TypeError: Raises if there is no related dremio instance

        Returns:
            list[Reflection]: List of all reflections of this dataset. Empty list if there is none.
        """
        if not self._dremio:
            raise TypeError(
                "This Dataset has no dremio instance!\nTry to refresh the dataset directly in the dremio sdk"
            )
        return self._dremio.get_reflections_from_dataset(self.path)

    @property
    def reflections(self) -> list[Reflection]:
        """All reflections of this dataset.

        Returns:
            list[Reflection]: List of all reflections of this dataset. Empty list if there is none.
        """
        return self.get_reflections()

    def refresh(self) -> None:
        """Refresh the dataset reflections. Same as `Dremio.refresh_dataset(this_dataset_path)`"""
        if not self._dremio:
            raise TypeError(
                "This Dataset has no dremio instance!\nTry to refresh the dataset directly in the dremio sdk"
            )
        self._dremio.refresh_dataset(self.path)
        self.pull()

    def delete_reflections(self) -> None:
        """Delete all reflections of this dataset."""
        if not self._dremio:
            raise TypeError(
                "This Dataset has no dremio instance!\nTry to refresh the dataset directly in the dremio sdk"
            )
        refs = self.get_reflections()
        for ref in refs:
            try:
                self._dremio.delete_reflection(ref.id)
            except Exception as e:
                logging.warning(e)
        return None

    def get_wiki(self) -> Wiki:
        """Get the wiki of the dataset."""
        if not self._dremio:
            raise TypeError(
                "This Dataset has no dremio instance!\nTry to get the wiki directly in the dremio sdk"
            )
        wiki = self._dremio.get_wiki(self.id)
        wiki._dataset_id = self.id
        wiki._dremio = self._dremio
        return wiki

    def set_wiki_text(self, text: str) -> Wiki:
        """Set the wiki text of the dataset."""
        if not self._dremio:
            raise TypeError(
                "This Dataset has no dremio instance!\nTry to set the wiki text directly in the dremio sdk"
            )
        wiki = self._dremio.get_wiki(self.id)
        wiki.text = text
        return wiki.commit()

    # maybe later: reason for not implementing this is that the wiki is not a property of the dataset and should be handled with the knowledge of the wiki object
    # @property
    # def wiki(self) -> Wiki:
    #   """Get the wiki of the dataset.
    #   """
    #   return self.get_wiki()

    # @wiki.setter
    # def wiki(self, wiki:Wiki|str):
    #   if not self._dremio:
    #     raise TypeError("This Dataset has no dremio instance!\nTry to set the wiki directly in the dremio sdk")
    #   if isinstance(wiki, str):
    #     self.set_wiki_text(wiki)
    #     return
    #   elif isinstance(wiki, Wiki):
    #     self.set_wiki_text(wiki.text)
    #     return
    #   raise TypeError("wiki must be a Wiki object or a string")

    def delete(self) -> None:
        """Delete the dataset. Same as `Dremio.delete_dataset(this_dataset_path)`."""
        if not self._dremio:
            raise TypeError(
                "This Dataset has no dremio instance!\nTry to delete the dataset directly in the dremio sdk"
            )
        success: bool = False
        try:
            success = self._dremio.delete_dataset(self.path)
        except Exception as e:
            raise e
        if not success:
            raise DremioConnectorError(
                "Dataset not deleted", f"Dataset {self.path} could not be deleted"
            )
        del self

    @property
    def schema(self) -> Schema[SQLType]:
        """Get schema like `JobResult.schema`. This is just a stream lined TypedDict for `.fields`

        Returns:
            Schema[SQLType]: `.fields` as TypedDict.
        """
        return Schema[SQLType](
            [
                SchemaField(name=f.name, type=SQLType(name=f.type.name))
                for f in self.fields
            ]
        )
