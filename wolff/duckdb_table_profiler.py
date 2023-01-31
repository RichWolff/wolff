import pandas as pd
from functools import lru_cache
from sqlalchemy import engine
from tqdm import tqdm
from typing import List, Tuple

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
    "double",
}
object_columns = {"char", "varchar", "text", "nchar", "nvarchar", "ntext"}
datetime_columns = {
    "date",
    "datetimeoffset",
    "datetime2",
    "smalldatetime",
    "datetime",
    "time",
    "timestamp",
}


class DuckDBTable:
    def __init__(self, sa_engine: engine, table: str):
        """_summary_

        Args:
            sa_engine (engine): Sql Alchemy Engine (create_engine())
            table (str): Table name at location
        """
        self.engine = sa_engine
        self._table = table

    @property
    def table(self):
        return f"'{self._table}'"

    @lru_cache(256)
    def info(self):
        qry = f"""DESCRIBE SELECT * FROM {self.table}"""
        return pd.read_sql(qry, self.engine).set_index("column_name")

    def sample(self, rows: int = 5):
        qry = f"SELECT * FROM {self.table} USING SAMPLE {rows};"
        return pd.read_sql(qry, self.engine)

    @property
    def shape(self):
        qry = f"SELECT COUNT(*) FROM {self.table}"
        return self.engine.execute(qry).fetchone()[0], self.info().shape[0]

    def describe(self, include="numeric", datetime_as_numeric=False):

        include_cols: List[Tuple(str, str)] = []

        if include in ("numeric", "all"):
            include_cols.extend(
                [
                    (self.column_profiler_numeric, col)
                    for col in self.info()[
                        self.info()["column_type"].str.lower().isin(numeric_columns)
                    ].index.tolist()
                ]
            )

        if datetime_as_numeric or include == "all":
            include_cols.extend(
                (self.column_profiler_datetime, col)
                for col in self.info()[
                    self.info()["column_type"].str.lower().isin(datetime_columns)
                ].index.tolist()
            )

        if include in ("object", "all"):
            include_cols.extend(
                (self.column_profiler_object, col)
                for col in self.info()[
                    self.info()["column_type"].str.lower().isin(object_columns)
                ].index.tolist()
            )

        return pd.concat(
            [
                pd.DataFrame(),
                *[profiler(column=col) for profiler, col in tqdm(include_cols)],
            ]
        ).set_index("column")

    def read(self):
        qry = f"SELECT * FROM {self.table}"
        return pd.read_sql(qry, self.engine)

    def compare_text_lengths(self):
        return self.describe(include="object").join(self.info())[
            ["column_type", "character_max_length", "computed_max_character_length"]
        ]

    @lru_cache(256)
    def column_profiler_object(self, column: str) -> pd.DataFrame:

        qry = f"""WITH cte_column_object AS (
                SELECT
                    "{column}",
                    COUNT(*) as occurrences
                FROM 
                    {self.table}
                WHERE
                    "{column}" is not null
                GROUP BY "{column}"
            )

            SELECT
                '{column}' as column
                ,max(len("{column}")) AS computed_max_character_length
                ,sum(occurrences) AS count
                ,count(*) as unique
                ,(SELECT "{column}" FROM cte_column_object ORDER BY occurrences DESC LIMIT 1) AS top
                ,max(occurrences) as freq
            FROM
                cte_column_object"""
        return pd.read_sql(qry, self.engine)

    @lru_cache(256)
    def column_profiler_numeric(self, column: str) -> pd.DataFrame:
        qry = f"""SELECT
                '{column}' as column,
                MIN("{column}") AS min,
                MAX("{column}") AS max,
                AVG("{column}") AS mean,
                STDDEV("{column}") AS std,
                quantile_disc("{column}", 0.25) AS "25%",
                quantile_disc("{column}", 0.5) AS "50%",
                quantile_disc("{column}", 0.75) AS "75%", 
                COUNT("{column}") AS count
            FROM {self.table}
            LIMIT 1"""
        return pd.read_sql(qry, self.engine)

    @lru_cache(256)
    def column_profiler_datetime(self, column):
        qry = f"""
            SELECT
                '{column}' AS column,
                MIN("{column}") AS min,
                MAX("{column}") AS max,
                TO_TIMESTAMP(CAST(AVG(EPOCH("{column}")) AS BIGINT)) AS mean,
                TO_TIMESTAMP(CAST(STDDEV(EPOCH("{column}")) AS BIGINT)) AS std,
                quantile_disc("{column}", 0.25) AS "25%",
                quantile_disc("{column}", 0.5) AS "50%",
                quantile_disc("{column}", 0.75) AS "75%",
                mode("{column}") as mode,
                COUNT("{column}") AS count
            FROM 
                {self.table}
            LIMIT 1
        """
        return pd.read_sql(qry, self.engine)

    def __repr__(self):
        # pks = ", ".join(self.info()self.info()["is_primary_key"] > 0].index)
        return f"Table(table='{self.table}', rows={self.shape[0]}, columns={self.shape[1]})'"
