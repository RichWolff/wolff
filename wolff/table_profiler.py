from sqlalchemy import engine
from wolff.duckdb_table_profiler import DuckDBTable
from wolff.sql_server_table_profiler import SqlServerTable


def Table(sa_engine: engine, table: str):
    if sa_engine.driver == "duckdb_engine":
        return DuckDBTable(sa_engine=sa_engine, table=table)

    elif sa_engine.driver == "pyodbc":
        return SqlServerTable(sa_engine=sa_engine, table=table)
