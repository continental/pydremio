__all__ = ["_MixinTable"]  # this is like `export ...` in typescript
import logging
import pandas as pd
import polars as pl
from datetime import datetime, date
from typing import Optional

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

def sql_merge_on_clause(on: dict[str, str] | str, source_var:str="s", target_var:str="t") -> str:
    """
    Converts a dictionary or string into a SQL ON clause for MERGE statements.
    
    Args:
        on: A dictionary mapping source column names to target column names, or a string representing the ON clause.
        source_var: The variable name for the source table (default is 's').
        target_var: The variable name for the target table (default is 't').
    
    Returns:
        A string representing the SQL ON clause.
    """
    if isinstance(on, dict):
        return " AND ".join([f"{target_var}.{k} = {source_var}.{v}" for k, v in on.items()])
    elif isinstance(on, str):
        return on
    else:
        raise TypeError("on must be a dict or a string.")

def dotted_full_path(path: list[str] | str, name: Optional[str] = None) -> str:
    path = path_to_list(path)
    return f"{'.'.join(path)}.{name}" if name else ".".join(path)

def warning_large_table_creation(df: pl.DataFrame) -> None:
    """
    Logs a warning if the DataFrame has more than 10,000 rows.
    
    Args:
        df: Polars DataFrame to check.
    """
    if len(df) > 10_000:
        logging.warning(
            "Creating a table with more than 10,000 rows may take a while. "
            "Consider using smaller batch sizes for better performance."
        )

class _MixinTable(_MixinQuery, _MixinFlight, _MixinDataset, _MixinSQL, BaseClass):
    def create_table_from_dataframe(
        self,
        path: list[str] | str,
        df: pd.DataFrame | pl.DataFrame,
        *,
        batch_size: int = 1000,
    ) -> None:
        """
        Creates an Iceberg table in Dremio from a Pandas DataFrame.

        Args:
            path: Path in the Dremio catalog where the table should be created.
            df: Pandas or Polars DataFrame to use for schema and data insertion.
        """

        if isinstance(df, pd.DataFrame):
            df = pl.from_pandas(df)
        if not isinstance(df, pl.DataFrame):
            raise TypeError("df must be a Pandas or Polars DataFrame.")
        warning_large_table_creation(df)

        # 1. Create table using DataFrame schema
        column_definitions = []
        for col in df.columns:
            sql_type = map_dtype_to_sql(df[col].dtype)
            column_definitions.append(f'"{col}" {sql_type}')
        columns_sql = ",\n  ".join(column_definitions)
        
        path = path_to_dotted(path)

        create_sql = f"""
        CREATE TABLE {path}
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
                    f"Table '{path}' already exists. Use update_dataset() to modify it."
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
                INSERT INTO {path} VALUES
                {",\n".join(value_rows)}
                """
                self.query(insert_sql)
                value_rows = []

        if value_rows:
            insert_sql = f"""
            INSERT INTO {path} VALUES
            {",\n".join(value_rows)}
            """
            self.query(insert_sql)

    def create_table_from_sql(self, path: list[str] | str, sql: str) -> None:
        """
        Creates an Iceberg table in Dremio from an SQL query.

        Args:
            path: Path in the Dremio catalog where the table should be created.
            sql: SQL query to use for creating the table via CTAS (CREATE TABLE AS SELECT).
        """

        if not isinstance(sql, str):
            raise TypeError("sql must be a string.")

        path = path_to_dotted(path)

        # Create table using SQL query
        create_sql = f"""
        CREATE TABLE {path} AS
        {sql}
        """
        try:
            self.query(create_sql)
        except DremioError as e:
            if e.status_code == 409:
                e.errorMessage = (
                    f"Table '{path}' already exists. Use update_table() to modify it."
                    + e.errorMessage
                )
                raise e

    def create_table(
        self,
        path: str,
        based_on: pd.DataFrame | pl.DataFrame | str,
        *,
        batch_size: int = 1000,
    ) -> None:
        """
        Creates an Iceberg table in Dremio either from a Pandas/Polars DataFrame or an SQL query.

        Args:
            based_on: Optional DataFrame or SQL-Statement to use for schema and data insertion.
            path: Path in the Dremio catalog where the table should be created.
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
                df=based_on, path=path, batch_size=batch_size
            )
        elif isinstance(based_on, str):
            self.create_table_from_sql(sql=based_on, path=path)
        else:
            raise TypeError(
                "from must be a Pandas DataFrame, Polars DataFrame or a SQL query string."
            )

    def update_table_from_sql(self, path: list[str]|str, sql: str, *, on: dict[str,str]|str = {'id':'id'}) -> None:
        """
        Updates or inserts rows into an existing Iceberg table in Dremio using MERGE INTO.
    
        Args:
            dremio: Dremio connection instance.
            path: Path in the Dremio catalog.
            sql: SQL query string as source.
            on: SQL ON clause string to define matching criteria (e.g., "t.id = s.id"). As string use "s." and "t." as prefix to indicate source and target column. As dict use {"target_column": "source_column"}. 
        Raises:
            RuntimeError: If the target table does not exist.
            DremioError: If Dremio returns an error.
        """
    
        if not isinstance(sql, str):
            raise TypeError("sql must be a string.")
        
        path = path_to_dotted(path)
        try:
            dataset = self.get_catalog_by_path(path)
            if not dataset:
                raise RuntimeError(f"Table '{path}' does not exist. Use create_table() instead.")
        except DremioError as e:
            if "No such file or directory" in str(e):
                e.errorMessage = f"Table '{path}' does not exist."
                raise e
            else:
                raise e
    
        # Use SQL query directly as source
        merge_sql = f"""
        MERGE INTO {path} AS t
        USING ({sql}) AS s
        ON ({sql_merge_on_clause(on, source_var='s', target_var='t')})
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        print(merge_sql)
        self.query(merge_sql)

    def update_table_from_dataframe(self, path: list[str]|str, df: pd.DataFrame | pl.DataFrame,  *, on: dict[str,str]|str = {'id':'id'}, batch_size: int = 1000, keep_temp_table: bool = False) -> None:
        """
        Updates or inserts rows into an existing Iceberg table in Dremio using MERGE INTO.
    
        Args:
            dremio: Dremio connection instance.
            path: Path in the Dremio catalog.
            df: DataFrame to use as source data.
            on: SQL ON clause string to define matching criteria (e.g., "t.id = s.id"). As string use "s." and "t." as prefix to indicate source and target column. As dict use {"target_column": "source_column"}. 
            batch_size: Number of rows to insert in each batch.
            keep_temp_table: If True, the temporary table will not be dropped after the merge operation.
        Raises:
            ValueError: If neither or both `df` and `sql` are provided.
            RuntimeError: If the target table does not exist.
        """

        if isinstance(df, pd.DataFrame):
            df = pl.from_pandas(df)
        if not isinstance(df, pl.DataFrame):
            raise TypeError("df must be a Pandas or Polars DataFrame.")
        warning_large_table_creation(df)
    
        # Create a temp table to use as the merge source
        path = path_to_list(path)
        table_name = path[-1]
        folder = path_to_dotted(path[0:-1])
        temp_table_path = path_to_dotted(f"{folder}.{table_name}_temp_update")
        self.create_table_from_dataframe(df=df, path=temp_table_path, batch_size=batch_size)

        merge_sql = f"""
        MERGE INTO {path_to_dotted(path)} AS t
        USING {temp_table_path} AS s
        ON ({sql_merge_on_clause(on, source_var='s', target_var='t')})
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
            except DremioError as e:
                raise e

    # def get_table_files_metadata(dremio: Dremio, path):
    #     return dremio.query(f"SELECT * FROM TABLE(table_files('{path}.{table_name}'))").to_pandas()
    
    # def get_table_history_metadata(dremio: Dremio, path, table_name):
    #     return dremio.query(f"SELECT * FROM TABLE(table_history('{path}.{table_name}'))").to_pandas()
    
    # def get_table_manifests_metadata(dremio: Dremio, path, table_name):
    #     return dremio.query(f"SELECT * FROM TABLE(table_manifests('{path}.{table_name}'))").to_pandas()
    
    # def get_table_partitions_metadata(dremio: Dremio, path, table_name):
    #     return dremio.query(f"SELECT * FROM TABLE(table_partitions('{path}.{table_name}'))").to_pandas()
    
    # def get_table_snapshot_metadata(dremio: Dremio, path, table_name):
    #     return dremio.query(f"SELECT * FROM TABLE(table_snapshot('{path}.{table_name}'))").to_pandas()
