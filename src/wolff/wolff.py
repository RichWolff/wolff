import operator as oper
from functools import reduce
import pandas as pd


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
