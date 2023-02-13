import pandas as pd
from functools import lru_cache
from sqlalchemy import engine
from typing import List, Tuple
from tqdm import tqdm
from sqlalchemy.engine import Connection
import pyodbc
import tempfile
import urllib
from wolff.wolff import download_sql_data
import os
from pathlib import Path


import contextlib
import pyarrow as pa
from pyarrow import parquet as pq
import shutil

pyodbc.pooling = False

numeric_columns = {
    "bigint",
    "numeric",
    "bit",
    "smallint",
    "decimal",
    "smallmoney",
    "int",
    "tinyint",
    "money",
}
object_columns = {"char", "varchar", "text", "nchar", "nvarchar", "ntext"}
datetime_columns = {
    "date",
    "datetimeoffset",
    "datetime2",
    "smalldatetime",
    "datetime",
    "time",
}


class SqlServerTable:
    def __init__(self, sa_engine: engine, table: str):
        """_summary_

        Args:
            sa_engine (engine): Sql Alchemy Engine (create_engine())
            table (str): Table name at location
        """
        self.engine = sa_engine
        self.table = table
        self.engine.update_execution_options(
            pool_reset_on_return=None,
        )

    # @event.listens_for(pyodbc, "reset")
    # def _reset_mssql(dbapi_connection, connection_record, reset_state):
    #     dbapi_connection.execute("{call sys.sp_reset_connection}")

    #     # so that the DBAPI itself knows that the connection has been
    #     # reset
    #     dbapi_connection.rollback()

    @lru_cache(256)
    def info(self):

        qry = f"""
            WITH cte_tbl_primary_key as (
                SELECT 
                    COLUMN_NAME,
                    CAST(1 as bit) as primary_key
                FROM 
                    INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE 
                    OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
                    and TABLE_NAME = '{self.table}'
            )

            SELECT 
                cols.COLUMN_NAME,
                COALESCE(pk.primary_key, 0) as PRIMARY_KEY,
                cols.DATA_TYPE, 
                CASE cols.IS_NULLABLE WHEN 'YES' THEN 1 ELSE 0 END AS [IS_NULLABLE], 
                cols.CHARACTER_MAXIMUM_LENGTH, 
                cols.NUMERIC_PRECISION, 
                cols.DATETIME_PRECISION 
            FROM
                INFORMATION_SCHEMA.COLUMNS as cols
            LEFT JOIN
                cte_tbl_primary_key as pk
            ON
                cols.column_name = pk.column_name
            WHERE
                TABLE_NAME = '{self.table}'
        """
        data_types = self.engine.execute(qry)
        rows = data_types.fetchall()
        profile = {
            row[0]: {
                "is_primary_key": row[1],
                "data_type": row[2],
                "is_nullable": row[3],
                "character_max_length": row[4],
                "numeric_precision": row[5],
                "datetime_precision": row[6],
            }
            for row in rows
        }
        return pd.DataFrame(profile).T

    def sample(self, rows: int = 5):
        qry = f"SELECT TOP {rows} * FROM {self.table} ORDER BY NEWID()"
        return pd.read_sql(qry, self.engine)

    @property
    def shape(self):
        qry = f"SELECT COUNT(*) FROM {self.table}"
        return self.engine.execute(qry).fetchone()[0], self.info().shape[0]

    def describe(self, include="numeric", datetime_as_numeric=False, sample=None):

        include_cols: List[Tuple(str, str)] = []

        if include in ("numeric", "all"):
            include_cols.extend(
                [
                    (self.column_profiler_numeric, col)
                    for col in self.info()[
                        self.info()["data_type"].isin(numeric_columns)
                    ].index.tolist()
                ]
            )

        if datetime_as_numeric or include == "all":
            include_cols.extend(
                (self.column_profiler_datetime, col)
                for col in self.info()[
                    self.info()["data_type"].isin(datetime_columns)
                ].index.tolist()
            )

        if include in ("object", "all"):
            include_cols.extend(
                (self.column_profiler_object, col)
                for col in self.info()[
                    self.info()["data_type"].isin(object_columns)
                ].index.tolist()
            )

        kwargs = {}
        if sample:
            if sample[-1] == "%":
                sample_size = int(self.shape[0] * int(sample[:-1]) / 100)
            elif isinstance(sample, float):
                sample_size = int(self.shape[0] * sample)
            elif isinstance(sample, int):
                sample_size = sample
            else:
                raise ValueError(
                    f'Sample should be expressed as ("#%", .#, #). You passed {sample}.'
                )

        with self.engine.connect() as cnxn:

            table = "#temp_table" if sample else self.table

            with cnxn.begin() as t:
                if sample:
                    cnxn.execute(f"DROP TABLE IF EXISTS {table};")
                    cnxn.execute(
                        f"SELECT TOP {sample_size} {', '.join(row[1] for row in include_cols)} INTO {table} FROM {self.table} ORDER BY NEWID()"
                    )

                result = pd.concat(
                    [
                        pd.DataFrame(),
                        *[
                            profiler(column=col, cnxn=cnxn, table=table)
                            for profiler, col in tqdm(include_cols)
                        ],
                    ]
                ).set_index("column")
        return self.info()[["data_type"]].join(result)

    def write_create_table(self):
        desc = self.describe(include="all")
        numeric_precision = (
            self.info()[
                ["character_max_length", "numeric_precision", "datetime_precision"]
            ]
            .max(axis=1)
            .to_frame("server_numeric_precision")
        )
        data_types = self.info()["data_type"]
        create_qry = f

    def read(self):
        qry = f"SELECT * FROM {self.table}"
        return pd.read_sql(qry, self.engine)

    def compare_text_lengths(self):
        return self.describe(include="object").join(self.info())[
            ["data_type", "character_max_length", "computed_max_character_length"]
        ]

    @lru_cache(256)
    def column_profiler_object(
        self, column: str, cnxn: Connection, table: str
    ) -> pd.DataFrame:

        qry = f"""WITH cte_column_object AS (
                SELECT
                    {column},
                    COUNT(*) as [occurrences]
                FROM 
                    {table}
                WHERE
                    {column} is not null
                GROUP BY {column}
            )

            SELECT
                '{column}' as [column]
                ,max(len({column})) AS [computed_max_character_length]
                ,sum([occurrences]) AS [count]
                ,count(*) as [unique]
                ,(SELECT TOP 1 {column} FROM cte_column_object ORDER BY occurrences DESC) AS [top]
                ,max([occurrences]) as freq
            FROM
                cte_column_object"""
        return pd.read_sql(qry, cnxn)

    @lru_cache(256)
    def column_profiler_numeric(
        self, column: str, cnxn: Connection, table: str
    ) -> pd.DataFrame:
        qry = f"""
            SELECT TOP 1
                '{column}' as [column],
                MIN([{column}]) OVER() AS [min],
                MAX([{column}]) OVER() AS [max],
                AVG([{column}]) OVER() AS [mean],
                STDEV([{column}]) OVER() AS [std],
                PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY [{column}]) OVER() AS [25%],
                PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY [{column}]) OVER() AS [50%],
                PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY [{column}]) OVER() AS [75%],
                COUNT([{column}]) OVER() AS [count],
                MAX(LEN([{column}])) OVER() AS [computed_max_character_length]
            FROM [{table}]"""
        return pd.read_sql(qry, cnxn)

    @lru_cache(256)
    def column_profiler_datetime(
        self, column: str, cnxn: Connection, table: str
    ) -> pd.DataFrame:
        qry = f"""
            SELECT TOP 1
                '{column}' as [column],
                MIN([{column}]) OVER() AS [min],
                MAX([{column}]) OVER() AS [max],
                CAST(AVG(CAST(CAST([{column}] AS DATETIME) as float)) OVER() AS datetime) AS [mean],
                CAST(STDEV(CAST(CAST([{column}] AS DATETIME) AS float)) OVER() AS datetime) AS [std],
                PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY [{column}]) OVER() AS [25%],
                PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY [{column}]) OVER() AS [50%],
                PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY [{column}]) OVER() AS [75%],
                COUNT([{column}]) OVER() AS [count]
            FROM 
                {table}
        """
        return pd.read_sql(qry, cnxn)

    def download_table(self, output_directory, auth_type="sql_login"):

        date_cols = self.info()[
            self.info()["data_type"].str.lower().isin(datetime_columns)
        ].index.values.tolist()
        string_cols = self.info()[
            self.info()["data_type"].str.lower().isin(object_columns)
        ].index.values.tolist()
        num_cols = self.info()[
            self.info()["data_type"].str.lower().isin(numeric_columns)
        ].index.values.tolist()

        columns = [*date_cols, *string_cols, *num_cols]
        query_cols = [
            *date_cols,
            *[f"""QUOTENAME({col}, '"') AS {col}""" for col in string_cols],
            *num_cols,
        ]
        qry = """SET NOCOUNT ON;\nSET QUOTED_IDENTIFIER ON\n\n"""
        qry += "SELECT\n\t" "" + ",\n\t".join(query_cols)
        qry += "\nFROM\n\t"
        qry += f"{self.table}"

        temp_qry_file = tempfile.mktemp(".sql")
        with open(temp_qry_file, "w") as f:
            f.write(qry)

        kwargs = {}

        for row in [
            row.split("=")
            for row in urllib.parse.parse_qsl(str(self.engine.url))[0][1].split(";")
        ]:
            if len(row) > 1:
                k, v = row
                if k.lower() == "server":
                    kwargs[k.lower()] = v
                elif k.lower() == "database":
                    kwargs[k.lower()] = v
                elif k.lower() == "uid":
                    kwargs[k.lower()] = v
                elif k.lower() == "pwd":
                    kwargs[k.lower()] = v

        export_file, result = download_sql_data(
            server=kwargs["server"],
            database=kwargs["database"],
            query_file=Path(f"{temp_qry_file}"),
            auth_type=auth_type,
            uid=kwargs["uid"],
            pwd=kwargs["pwd"],
            sep="|",
        )

        df = pd.read_csv(
            filepath_or_buffer=export_file,
            sep="|",
            header=None,
            names=columns,
            # dtype=dtypes,
            parse_dates=date_cols,
            keep_default_na=False,
            na_values=[
                "",
                "#N/A",
                "#N/A N/A",
                "#NA",
                "-1.#IND",
                "-1.#QNAN",
                "-NaN",
                "-nan",
                "1.#IND",
                "1.#QNAN",
                "<NA>",
                "NA",
                "NULL",
                "NaN",
                "N/A",
                "nan",
                "null",
            ],
        )
        output_directory = Path(output_directory)
        with contextlib.suppress(Exception):
            tempdir = tempfile.mkdtemp()
            tbl = pa.Table.from_pandas(df)
            pq.write_to_dataset(tbl, tempdir, row_group_size=1000000)
        try:
            shutil.rmtree(output_directory / self.table)
            shutil.move(tempdir, output_directory / self.table)
        except Exception:
            shutil.move(tempdir, output_directory / self.table)

    def __repr__(self):
        pks = ", ".join(self.info()[self.info()["is_primary_key"] > 0].index)
        return f"Table(table='{self.table}', primary_key='{pks}', rows={self.shape[0]}, columns={self.shape[1]})'"
