__all__ = ["_MixinTable"]  # this is like `export ...` in typescript
import pandas as pd
import polars as pl
from datetime import datetime, date
from typing import Optional

from traitlets import Bool

from ..utils.converter import path_to_dotted, path_to_list
from ..exceptions import DremioError


from . import BaseClass
from ._dataset import _MixinDataset
from ._sql import _MixinSQL
from ._flight import _MixinFlight
from ._query import _MixinQuery


def map_dtype_to_sql(dtype: pl.DataType) -> str:
    """Maps Polars dtype to Dremio SQL data type."""
    if dtype == pl.Int8 or dtype == pl.Int16 or dtype == pl.Int32 or dtype == pl.Int64:
        return "BIGINT"
    elif (
        dtype == pl.UInt8
        or dtype == pl.UInt16
        or dtype == pl.UInt32
        or dtype == pl.UInt64
    ):
        return "BIGINT"
    elif dtype == pl.Float32 or dtype == pl.Float64:
        return "DOUBLE"
    elif dtype == pl.Boolean:
        return "BOOLEAN"
    elif dtype == pl.Datetime or dtype == pl.Date:
        return "TIMESTAMP"
    else:
        return "VARCHAR"


def escape_sql_value(val) -> str:
    """Escapes and formats a value for SQL insertion."""
    if val is None:
        return "NULL"
    elif isinstance(val, float) and (val != val):  # NaN check
        return "NULL"
    elif isinstance(val, str):
        val_escaped = val.replace("'", "''")
        return f"'{val_escaped}'"
    elif isinstance(val, (datetime, date)):
        return f"TIMESTAMP '{val.isoformat(sep=' ', timespec='seconds')}'" if isinstance(val, datetime) \
            else f"DATE '{val.isoformat()}'"
    elif isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    else:
        return str(val)


def dotted_full_path(path: list[str] | str, name: Optional[str] = None) -> str:
    path = path_to_list(path)
    return f"{'.'.join(path)}.{name}" if name else ".".join(path)


class _MixinTable(_MixinQuery, _MixinFlight, _MixinDataset, _MixinSQL, BaseClass):
    def create_table_from_dataframe(
        self,
        df: pd.DataFrame | pl.DataFrame,
        path: list[str] | str,
        *,
        name: Optional[str] = None,
        batch_size: int = 1000,
    ) -> None:
        """
        Creates an Iceberg table in Dremio from a Pandas DataFrame.

        Args:
            path: Path in the Dremio catalog where the table should be created.
            name: Name of the new table.
            df: Pandas or Polars DataFrame to use for schema and data insertion.
        """

        if isinstance(df, pd.DataFrame):
            df = pl.from_pandas(df)
        if not isinstance(df, (pd.DataFrame, pl.DataFrame)):
            raise TypeError("df must be a Pandas or Polars DataFrame.")
        full_table_path = dotted_full_path(path, name)

        # 1. Create table using DataFrame schema
        column_definitions = []
        for col in df.columns:
            sql_type = map_dtype_to_sql(df[col].dtype)
            column_definitions.append(f'"{col}" {sql_type}')
        columns_sql = ",\n  ".join(column_definitions)

        create_sql = f"""
        CREATE TABLE {full_table_path}
        (
          {columns_sql}
        )
        """
        # will be only created if table not exists!
        try:
            self.query(create_sql)
            pass
        except DremioError as e:
            if e.status_code == 409:
                e.errorMessage = (
                    f"Table '{full_table_path}' already exists. Use update_dataset() to modify it."
                    + e.errorMessage
                )
                raise e

        # 2. Batch insert rows
        value_rows = []

        for row in df.iter_rows(named=False):
            values = ", ".join(escape_sql_value(val) for val in row)
            value_rows.append(f"({values})")
            if len(value_rows) >= batch_size:
                insert_sql = f"""
                INSERT INTO {full_table_path} VALUES
                {",\n".join(value_rows)}
                """
                self.query(insert_sql)
                value_rows = []

        if value_rows:
            insert_sql = f"""
            INSERT INTO {full_table_path} VALUES
            {",\n".join(value_rows)}
            """
            self.query(insert_sql)

    def create_table_from_sql(
        self, sql: str, path: list[str] | str, name: Optional[str] = None
    ) -> None:
        """
        Creates an Iceberg table in Dremio from an SQL query.

        Args:
            path: Path in the Dremio catalog where the table should be created.
            name: Name of the new table.
            sql: SQL query to use for creating the table via CTAS (CREATE TABLE AS SELECT).
        """

        if not isinstance(sql, str):
            raise TypeError("sql must be a string.")
        full_table_path = dotted_full_path(path, name)

        # Create table using SQL query
        create_sql = f"""
        CREATE TABLE {full_table_path} AS
        {sql}
        """
        try:
            self.query(create_sql)
        except DremioError as e:
            if e.status_code == 409:
                e.errorMessage = (
                    f"Table '{full_table_path}' already exists. Use update_table() to modify it."
                    + e.errorMessage
                )
                raise e

    def create_table(
        self,
        based_on: pd.DataFrame | pl.DataFrame | str,
        path: str,
        name: Optional[str] = None,
        *,
        batch_size: int = 1000,
    ) -> None:
        """
        Creates an Iceberg table in Dremio either from a Pandas/Polars DataFrame or an SQL query.

        Args:
            based_on: Optional DataFrame or SQL-Statement to use for schema and data insertion.
            path: Path in the Dremio catalog where the table should be created.
            name: Name of the new table.
        Raises:
            ValueError: If neither or both `df` and `sql` are provided.
            RuntimeError: If the table already exists.
        """

        if based_on is None:
            raise ValueError(
                "You must provide either a DataFrame or a SQL query to create the table."
            )
        if isinstance(based_on, (pd.DataFrame, pl.DataFrame)):
            self.create_table_from_dataframe(
                df=based_on, path=path, name=name, batch_size=batch_size
            )
        elif isinstance(based_on, str):
            self.create_table_from_sql(sql=based_on, path=path, name=name)
        else:
            raise TypeError(
                "from must be a Pandas DataFrame, Polars DataFrame or a SQL query string."
            )

    def update_table_from_sql(self, sql: str, on: str, path: str) -> None:
        """
        Updates or inserts rows into an existing Iceberg table in Dremio using MERGE INTO.
    
        Args:
            dremio: Dremio connection instance.
            path: Path in the Dremio catalog.
            sql: SQL query string as source.
            on: SQL ON clause string to define matching criteria (e.g., "t.id = s.id"). Use "s." and "t." as prefix to indicate source and target column. 
        Raises:
            ValueError: If neither or both `df` and `sql` are provided.
            RuntimeError: If the target table does not exist.
        """
    
        if not isinstance(sql, str):
            raise TypeError("sql must be a string.")
        
        try:
            dataset = self.get_catalog_by_path(path)
            if not dataset:
                raise RuntimeError(f"Table '{path}' does not exist. Use create_table() instead.")
        except DremioError as e:
            if "No such file or directory" in str(e):
                raise RuntimeError(f"Table '{path}' does not exist.")
            else:
                raise
    
        # Use SQL query directly as source
        merge_sql = f"""
        MERGE INTO {path} AS t
        USING ({sql}) AS s
        ON ({on})
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        print(merge_sql)
        self.query(merge_sql)

    def update_table_from_dataframe(self, df: pd.DataFrame | pl.DataFrame, on: str, path: str, batch_size: int = 1000, keep_temp_table: Bool = False) -> None:
        """
        Updates or inserts rows into an existing Iceberg table in Dremio using MERGE INTO.
    
        Args:
            dremio: Dremio connection instance.
            path: Path in the Dremio catalog.
            df: DataFrame to use as source data.
        Raises:
            ValueError: If neither or both `df` and `sql` are provided.
            RuntimeError: If the target table does not exist.
        """

        if isinstance(df, pd.DataFrame):
            df = pl.from_pandas(df)
        if not isinstance(df, (pd.DataFrame, pl.DataFrame)):
            raise TypeError("df must be a Pandas or Polars DataFrame.")
    
        if df is not None:
            # Create a temp table to use as the merge source
            table_name = path.split('.')[-1]
            folder = path_to_dotted(path.split('.')[0:-1])
            print(folder)
            temp_table_path = f"{folder}.{table_name}_temp_update"
            self.create_table_from_dataframe(df, temp_table_path, batch_size=batch_size)
    
            merge_sql = f"""
            MERGE INTO {path} AS t
            USING {temp_table_path} AS s
            ON ({on})
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """  
            try:
                self.query(merge_sql)
                pass
            except DremioError as e:
                raise e
            
            if not keep_temp_table:
                try:
                    drop_sql = f"DROP TABLE {temp_table_path}"
                    self.query(drop_sql)
                    pass
                except DremioError as e:
                    raise e

    # def get_table_files_metadata(dremio: Dremio, path, table_name):
    #     return dremio.query(f"SELECT * FROM TABLE(table_files('{path}.{table_name}'))").to_pandas()
    #
    # def get_table_history_metadata(dremio: Dremio, path, table_name):
    #     return dremio.query(f"SELECT * FROM TABLE(table_history('{path}.{table_name}'))").to_pandas()
    #
    # def get_table_manifests_metadata(dremio: Dremio, path, table_name):
    #     return dremio.query(f"SELECT * FROM TABLE(table_manifests('{path}.{table_name}'))").to_pandas()
    #
    # def get_table_partitions_metadata(dremio: Dremio, path, table_name):
    #     return dremio.query(f"SELECT * FROM TABLE(table_partitions('{path}.{table_name}'))").to_pandas()
    #
    # def get_table_snapshot_metadata(dremio: Dremio, path, table_name):
    #     return dremio.query(f"SELECT * FROM TABLE(table_snapshot('{path}.{table_name}'))").to_pandas()
