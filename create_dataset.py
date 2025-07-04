from dremio.exceptions import DremioError
from src.dremio import Dremio
from dotenv import load_dotenv
import pandas as pd
import polars as pl
from src.dremio.exceptions import DremioError
from typing import Optional


load_dotenv()
dremio = Dremio.from_env()
dremio.flight_config.tls = True


def map_dtype_to_sql(dtype) -> str:
    """Maps Pandas dtype to Dremio SQL data type."""
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    elif pd.api.types.is_float_dtype(dtype):
        return "DOUBLE"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "VARCHAR"

def escape_sql_value(val):
    """Escapes and formats a value for SQL insertion."""
    if pd.isna(val):
        return "NULL"
    elif isinstance(val, str):
        val_escaped = val.replace("'", "''")
        return f"'{val_escaped}'"
    elif isinstance(val, pd.Timestamp):
        return f"TIMESTAMP '{val.isoformat(sep=' ', timespec='seconds')}'"
    elif isinstance(val, bool):
        return 'TRUE' if val else 'FALSE'
    else:
        return str(val)

def create_table(dremio: Dremio, target_path: str, table_name: str, df: Optional[pd.DataFrame | pl.DataFrame] = None, sql: Optional[str] = None, batch_size: int = 1000) -> None:
    """
    Creates an Iceberg table in Dremio either from a Pandas DataFrame or an SQL query.
    
    Args:
        dremio: Dremio connection instance.
        target_path: Path in the Dremio catalog where the table should be created.
        table_name: Name of the new table.
        df: Optional DataFrame to use for schema and data insertion.
        sql: Optional SQL query to use for creating the table via CTAS.
    Raises:
        ValueError: If neither or both `df` and `sql` are provided.
        RuntimeError: If the table already exists.
    """

    # TODO: Pandas -> Polars

    if df is None and sql is None:
        raise ValueError("You must provide either a DataFrame or a SQL query to create the table.")
    if df is not None and sql is not None:
        raise ValueError("Provide only one of DataFrame or SQL query, not both.")

    full_table_path = f"{target_path}.{table_name}"

    if df is not None:
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
            dremio.query(create_sql)
        except DremioError as e:
            if e.status_code == 409:
                e.errorMessage = f"Table '{full_table_path}' already exists. Use update_dataset() to modify it." + e.errorMessage
                raise e

        # 2. Batch insert rows
        value_rows = []

        for _, row in df.iterrows():
            values = ", ".join(escape_sql_value(val) for val in row)
            value_rows.append(f"({values})")
            if len(value_rows) >= batch_size:
                insert_sql = f"""
                INSERT INTO {full_table_path} VALUES
                {',\n'.join(value_rows)}
                """
                dremio.query(insert_sql)
                value_rows = []

        if value_rows:
            insert_sql = f"""
            INSERT INTO {full_table_path} VALUES
            {',\n'.join(value_rows)}
            """
            dremio.query(insert_sql)

    elif sql is not None:
        # Create table using SQL query
        create_sql = f"""
        CREATE TABLE {full_table_path} AS
        {sql}
        """
        dremio.query(create_sql)

def update_table(dremio: Dremio, target_path: str, table_name: str, on: str, df: Optional[pd.DataFrame | pl.DataFrame] = None, sql: Optional[str] = None) -> None:
    """
    Updates or inserts rows into an existing Iceberg table in Dremio using MERGE INTO.

    Args:
        dremio: Dremio connection instance.
        target_path: Path in the Dremio catalog.
        table_name: Target table to merge into.
        on: SQL ON clause string to define matching rows (e.g., "t.id = s.id").
        df: Optional DataFrame to use as source data.
        sql: Optional SQL query string as source.
    Raises:
        ValueError: If neither or both `df` and `sql` are provided.
        RuntimeError: If the target table does not exist.
    """

    if df is None and sql is None:
        raise ValueError("You must provide either a DataFrame or a SQL query to update the table.")
    if df is not None and sql is not None:
        raise ValueError("Provide only one of DataFrame or SQL query, not both.")

    full_table_path = f"{target_path}.{table_name}"

    # Check if the table exists
    # TODO: see above!
    try:
        dataset = dremio.get_catalog_by_path(full_table_path)
        if not dataset:
            raise RuntimeError(f"Table '{full_table_path}' does not exist. Use create_table() instead.")
    except DremioError as e:
        if "No such file or directory" in str(e):
            raise RuntimeError(f"Table '{full_table_path}' does not exist.")
        else:
            raise

    if df is not None:
        # Create a temp table to use as the merge source
        temp_table_name = f"{table_name}_temp_update"
        create_table(dremio, target_path, temp_table_name, df=df)

        merge_sql = f"""
        MERGE INTO {full_table_path} AS t
        USING {target_path}.{temp_table_name} AS s
        ON ({on})
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        dremio.query(merge_sql)

        # Optionally drop temp table
        # TODO: Add argument for keep_temp_table -> Default: False
        drop_sql = f"DROP TABLE {target_path}.{temp_table_name}"
        dremio.query(drop_sql)

    elif sql is not None:
        # Use SQL query directly as source
        merge_sql = f"""
        MERGE INTO {full_table_path} AS t
        USING ({sql}) AS s
        ON ({on})
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        dremio.query(merge_sql)

def get_table_files_metadata(dremio: Dremio, path, table_name):
    return dremio.query(f"SELECT * FROM TABLE(table_files('{path}.{table_name}'))").to_pandas()

def get_table_history_metadata(dremio: Dremio, path, table_name):
    return dremio.query(f"SELECT * FROM TABLE(table_history('{path}.{table_name}'))").to_pandas()

def get_table_manifests_metadata(dremio: Dremio, path, table_name):
    return dremio.query(f"SELECT * FROM TABLE(table_manifests('{path}.{table_name}'))").to_pandas()

def get_table_partitions_metadata(dremio: Dremio, path, table_name):
    return dremio.query(f"SELECT * FROM TABLE(table_partitions('{path}.{table_name}'))").to_pandas()

def get_table_snapshot_metadata(dremio: Dremio, path, table_name):
    return dremio.query(f"SELECT * FROM TABLE(table_snapshot('{path}.{table_name}'))").to_pandas()
  

### MAIN
target_path = 'BUCKET.TABLE'
target_table = 'test_ib_2'
# Sample DataFrame
df = pd.DataFrame({
    'id': [1, 2],
    'name': ['Alice', 'Bob'],
    'created_at': pd.to_datetime(['2023-01-01', '2023-01-02']),
    'active': [True, False]
    })

# # COMMANDS
# df = dremio.query('SELECT view_id, view_name FROM sys.views LIMIT 10').to_pandas()
# create_table(dremio, target_path, target_table, sql="SELECT view_id, view_name FROM sys.views")
# update_table(dremio, target_path, target_table, on='t.view_id = s.view_id' , sql='SELECT view_id, view_name FROM sys.views')

# # META DATA
# print(get_table_history_metadata(dremio, target_path, target_table))
# print(get_table_snapshot_metadata(dremio, target_path, target_table))
# print(get_table_files_metadata(dremio, target_path, target_table))
# print(get_table_manifests_metadata(dremio, target_path, target_table))
# print(get_table_partitions_metadata(dremio, target_path, target_table))

# # COUNTS
# print('count: ', dremio.query(f"SELECT count(*) FROM {target_path}.{target_table} AT '2025-06-19 12:30:00.000'"))
# print('count: ', dremio.query(f"SELECT count(*) FROM {target_path}.{target_table} AT SNAPSHOT '7668377959096568453'"))
# print('count: ', dremio.query(f"SELECT count(*) FROM {target_path}.{target_table} AT SNAPSHOT '3908417501096364230'"))