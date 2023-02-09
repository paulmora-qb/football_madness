"""Creating flag features"""

import pyspark
from pyspark.sql import functions as f


def expr_flag(expr: str, column_name: str) -> pyspark.sql.Column:
    """_summary_

    Args:
        expr (str): _description_
        column_name (str): _description_

    Returns:
        pyspark.sql.Column: _description_
    """
    return f.expr(expr).alias(column_name)
