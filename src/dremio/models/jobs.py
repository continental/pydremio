from dataclasses import dataclass
from datetime import datetime
from uuid import UUID
from typing import Literal, Optional, Any, TypedDict, Union, List, Dict, overload
from .schema import Schema, SQLType, SchemaField

import polars as pl
import pyarrow


@dataclass
class ReflectionRelationship:
    datasetId: UUID
    reflectionId: UUID
    relationship: Literal["CONSIDERED", "MATCHED", "CHOSEN"]


@dataclass
class Acceleration:
    reflectionRelationships: list[ReflectionRelationship]


@dataclass
class Job:
    jobState: Literal["COMPLETED", "CANCELED", "FAILED", "RUNNING"]
    errorMessage: str
    startedAt: datetime
    queryType: Literal[
        "UI_RUN",
        "UI_PREVIEW",
        "UI_INTERNAL_PREVIEW",
        "UI_INTERNAL_RUN",
        "UI_EXPORT",
        "ODBC",
        "JDBC",
        "REST",
        "ACCELERATOR_CREATE",
        "ACCELERATOR_DROP",
        "UNKNOWN",
        "PREPARE_INTERNAL",
        "ACCELERATOR_EXPLAIN",
        "UI_INITIAL_PREVIEW",
    ]
    cancellationReason: str
    queueId: Optional[str] = None
    resourceSchedulingEndedAt: Optional[datetime] = None
    resourceSchedulingStartedAt: Optional[datetime] = None
    rowCount: Optional[int] = None
    queueName: Optional[str] = None
    endedAt: Optional[datetime] = None
    acceleration: Optional[Acceleration] = None
    id: Optional[UUID] = None


class JobResultDict(TypedDict):
    rowCount: int
    schema: list[SchemaField]
    rows: list[dict[str, Any]]


@dataclass
class JobResult:
    """This is a job result object.
    You can run `json()`, `to_polars()` or `to_pandas()` on the result to use the data.
    """

    rowCount: int
    schema: Schema[pyarrow.DataType] | Schema[SQLType]
    rows: list[dict[str, Any]]
    # jobId: Optional[UUID] = None

    @staticmethod
    def from_arrow_table(arrow_table: pyarrow.Table) -> "JobResult":
        schema_names: list[str] = arrow_table.schema.names
        schema_types: list[pyarrow.DataType] = arrow_table.schema.types
        schema = Schema[pyarrow.DataType](
            [{"name": n, "type": t} for n, t in zip(schema_names, schema_types)]
        )
        rows = arrow_table.to_pylist()
        rowCount = len(rows)
        return JobResult(rowCount=rowCount, schema=schema, rows=rows)

    @staticmethod
    def from_dict(d: JobResultDict) -> "JobResult":
        schema = Schema[SQLType](d["schema"])
        return JobResult(rowCount=d["rowCount"], rows=d["rows"], schema=schema)

    @property
    def dict(self) -> JobResultDict:
        return {
            "rowCount": self.rowCount,
            "schema": list(self.schema),
            "rows": self.rows,
        }

    def preview_table(self, number_of_rows: int = 5) -> str:
        """Returns a preview table as string

        Args:
            number_of_rows (int, optional): number of rows, that should be included. -1 for all rows. Defaults to 5.

        Returns:
            str: table as printable string
        """
        from prettytable import PrettyTable

        t = PrettyTable(self.schema.names)
        t.align = "l"
        t.add_row(self.schema.type_names)
        t.add_divider()
        for r in self.rows[:number_of_rows]:
            t.add_row(list(r.values()))
        if number_of_rows < self.rowCount:
            t.add_row(["..."] * len(self.schema.names))
        return t.get_string()

    def __str__(self) -> str:
        return (
            f"\nlen: {self.rowCount}\tcols: {len(self.schema.names)}\n\n"
            + self.preview_table()
            + f"\n\nhint: run `.to_polars()` or `.to_pandas()` for more options\n"
        )

    def __repr__(self) -> str:
        return self.preview_table(50)

    @overload
    def __getitem__(self, key: str) -> List[Any]:
        """Get a specific column as list

        Args:
            key (str): column name

        Returns:
            List[Any]: column content as list
        """

    @overload
    def __getitem__(self, key: int) -> Dict[str, Any]:
        """Get a specific row as dict.

        Args:
            key (int): row number

        Returns:
            Dict[str, Any]: row content as dict
        """

    @overload
    def __getitem__(self, key: slice) -> List[Dict[str, Any]]:
        """Get a specific row as dict.

        Args:
            key (int): row number

        Returns:
            Dict[str, Any]: row content as dict
        """

    def __getitem__(
        self, key: str | int | slice
    ) -> List[Any] | Dict[str, Any] | List[Dict[str, Any]]:
        if (
            not isinstance(key, int)
            and not isinstance(key, str)
            and not isinstance(key, slice)
        ):
            raise TypeError("key has to be int, str or slice")

        if isinstance(key, int) or isinstance(key, slice):
            return self.rows[key]

        if key not in self.schema.names:
            raise KeyError(f"Key '{key}' not found in schema.")
        index = self.schema.names.index(key)
        col = self.to_polars().to_series(index)
        return col.to_list()

    def __iter__(self):
        return iter(self.rows)

    def __next__(self):
        for row in self.rows:
            yield row

    def __len__(self):
        return self.rowCount

    def json(self, indent: int = 2, *args, **kwargs) -> str:
        """Convert result into a json string."""
        import json

        return json.dumps(self.dict, indent=indent, *args, **kwargs)

    def to_polars(self) -> pl.DataFrame:
        """Convert result into a polars data frame.

        [Polars Docs](https://docs.pola.rs/py-polars/html/reference/dataframe/index.html)
        """
        return pl.DataFrame(self.rows)

    def to_pandas(self):
        """Convert result into a pandas data frame, but we recommend to use `to_polars`

        [Pandas Docs](https://pandas.pydata.org/docs/getting_started/intro_tutorials/03_subset_data.html#how-do-i-select-specific-columns-from-a-dataframe)
        """
        import pandas as pd

        return pd.DataFrame(self.rows)
