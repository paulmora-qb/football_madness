"""Creating flag features"""

import pyspark
from pyspark.sql import functions as f


def expr_flag(expr: str, column_name: str) -> pyspark.sql.Column:
    """Function to create pyspark columns through the expression function. That allows
    to use sql-style features

    Args:
        expr (str): SQL syntax based features
        column_name (str): Name of the output column

    Returns:
        pyspark.sql.Column: Output column
    """
    return f.expr(expr).alias(column_name)
