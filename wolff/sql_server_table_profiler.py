import pandas as pd
from functools import lru_cache

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
    def __init__(self, sa_engine, table):
        self.engine = sa_engine
        self.table = table

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

    def describe(self, include="numeric", datetime_as_numeric=False):

        result = pd.DataFrame()

        if include in ("numeric", "all"):
            if num_cols := tuple(
                self.info()[self.info()["data_type"].isin(numeric_columns)].index.values
            ):

                result = pd.concat(
                    [
                        result,
                        self.column_profiler_numeric(num_cols),
                    ]
                )

            if datetime_as_numeric or include == "all":
                if num_cols := tuple(
                    self.info()[
                        self.info()["data_type"].isin(datetime_columns)
                    ].index.values
                ):

                    result = pd.concat(
                        [
                            result,
                            self.column_profiler_datetime(num_cols),
                        ]
                    )

        if include in ("object", "all"):
            if num_cols := tuple(
                self.info()[self.info()["data_type"].isin(object_columns)].index.values
            ):

                result = pd.concat(
                    [
                        result,
                        self.column_profiler_object(num_cols),
                    ]
                )
        return result

    def compare_text_lengths(self):
        return self.describe(include="object").join(self.info())[
            ["data_type", "character_max_length", "max_character_length"]
        ]

    @lru_cache(256)
    def column_profiler_object(self, columns):
        df = pd.DataFrame()
        for column in columns:
            qry = f"""WITH cte_column_object AS (
                SELECT
                    {column},
                    COUNT(*) as [occurrences]
                FROM 
                    {self.table}
                WHERE
                    {column} is not null
                GROUP BY {column}
            )

            SELECT
                '{column}' as [column]
                ,max(len({column})) AS [max_character_length]
                ,sum([occurrences]) AS [count]
                ,count(*) as [unique]
                ,(SELECT TOP 1 {column} FROM cte_column_object ORDER BY occurrences DESC) AS [top]
                ,max([occurrences]) as freq
            FROM
                cte_column_object"""
            df = pd.concat([df, pd.read_sql(qry, self.engine)])
        return df.set_index("column")

    @lru_cache(256)
    def column_profiler_numeric(self, columns):
        df = pd.DataFrame()
        for column in columns:
            qry = f"""WITH cte_stats AS (
                SELECT TOP 1
                    MIN([{column}]) OVER() AS [min],
                    MAX([{column}]) OVER() AS [max],
                    AVG([{column}]) OVER() AS [mean],
                    STDEV([{column}]) OVER() AS [std],
                    PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY [{column}]) OVER() AS [25%],
                    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY [{column}]) OVER() AS [50%],
                    PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY [{column}]) OVER() AS [75%],
                    COUNT([{column}]) OVER() AS [count]
                FROM [{self.table}]
            )
            SELECT
                '{column}' as [column],
                [min],
                [max],
                [mean],
                [std],
                [25%],
                [50%],
                [75%],
                [count]
            FROM cte_stats"""
            df = pd.concat([df, pd.read_sql(qry, self.engine)])
        return df.set_index("column")

    @lru_cache(256)
    def column_profiler_datetime(self, columns):
        df = pd.DataFrame()
        for column in columns:
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
                    {self.table}
            """
            df = pd.concat([df, pd.read_sql(qry, self.engine)])
        return df.set_index("column")

    def __repr__(self):
        pks = ", ".join(self.info()[self.info()["is_primary_key"] > 0].index)
        return f"Table(table='{self.table}', primary_key='{pks}', rows={self.shape[0]}, columns={self.shape[1]})'"
