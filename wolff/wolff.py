"""Main module."""
import operator as oper
from functools import reduce
import pandas as pd
import subprocess
from pathlib import Path
import tempfile
import uuid


operator_factory = {
    "=": lambda x, y: oper.eq(x, y),
    "==": lambda x, y: oper.eq(x, y),
    ">": lambda x, y: oper.gt(x, y),
    ">=": lambda x, y: oper.ge(x, y),
    "<": lambda x, y: oper.lt(x, y),
    "<=": lambda x, y: oper.le(x, y),
    "in": lambda x, y: x.isin(y),
    "not in": lambda x, y: ~x.isin(y),
    "contains": lambda x, y: x.str.contains(str(y)),
    "not contains": lambda x, y: ~x.str.contains(str(y)),
}


def filter_pd(df, filters=None):

    if not filters:
        return df

    # If nesting level is only one, then add one to make 2
    # We have encountered the situation where we have one nesting level
    # too few:
    #   We have [(,,), ..] instead of [[(,,), ..]]

    if isinstance(filters[0][0], str):
        filters = [filters]

    indice_names = df.index.names
    column_names = [] if isinstance(df, (pd.Index, pd.Series)) else df.columns
    series_name = df.name if isinstance(df, (pd.Series)) else ""

    disjunction_members = []  # OR
    for conjunction in filters:
        conjunction_members = []
        for col, op, val in conjunction:
            if col in indice_names:
                series = df.index.get_level_values(col)
            elif col in column_names:
                series = df[col]
            elif col == series_name:
                series = df
            else:
                raise ValueError(f"Column '{col}' is not in the passed dataframe")
            conjunction_members.append(operator_factory[op](series, val))
        # Loop through filters the test
        disjunction_members.append(
            reduce(oper.and_, conjunction_members)
        )  # equivalent to all()
    return df[reduce(oper.or_, disjunction_members)]  # equivalent to any()


def download_sql_data(
    server: str,
    database: str,
    query_file: Path,
    output_file: Path = None,
    auth_type: str = "sql_login",
    uid: str = None,
    pwd: str = None,
    sep: str = "|",
) -> subprocess.Popen.communicate:
    """Download SQL Data From SQL Server using SQLCMD with AZURE AD auth.

    Args:
        server (str): Server path
        database (str): Database Name
        query_file (Path): Path to the file containing the query
        output_file (Path): Output TSV to save. If None, a temporary output file will be generated. Defaults to None.
        auth_type (str): Authentication Type. One of 'sql_login', 'windows_auth', 'add'. Defaults to 'sql_login'.
        uid (str): Your User ID. Defaults to None.
        pwd (str): Your Password. Defaults to None.
        sep (str): Column Separator. Defaults to '|'.

    Returns:
        (output_file, subprocess.Popen.communicate): Tuple of output file and the Communication output from POPEN Process
    """

    auth_types = {"sql_login", "windows_auth", "add"}
    if auth_type not in auth_types:
        raise ValueError(
            f"Argument auth_type must be one of ({auth_types}). You passed '{auth_type}'"
        )

    if auth_type in {"sql_login", "add"} and ((not uid) or (not pwd)):
        raise ValueError(
            "Username and Password required when using auth_type of '{auth_type}'."
        )

    # Generate output file if none
    if output_file is None:
        output_file = Path(tempfile.mkdtemp()) / f"{uuid.uuid1()}.csv"

    output_path = output_file.parent
    output_file_name = output_file.name
    subprocess_args = [
        "sqlcmd",
        "-S",
        f"{server}",
        "-d",
        f"{database}",
        "-i",
        f"{str(query_file)}",
        "-o",
        f"{output_file_name}",
        "-W",  # REMOVE TRAILING SPACES
        "-I",  # (enable quoted identifiers)
        "-h",  # ROWS PER HEADER
        "-1",  # ROWS PER HEADER
        "-s",  # COL SEPARATOR
        f"{sep}",  # COL SEPARATOR
    ]

    # Auth Arguments
    if auth_type == "add":
        subprocess_args.extend(["-G", "-U", f"{uid}", "-P", f"{pwd}"])
    elif auth_type == "sql_login":
        subprocess_args.extend(["-U", f"{uid}", "-P", f"{pwd}"])
    elif auth_type == "windows_auth":
        subprocess_args.extend(["-E"])

    process = subprocess.Popen(
        subprocess_args,
        cwd=str(output_path),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    return (output_file, process.communicate())
