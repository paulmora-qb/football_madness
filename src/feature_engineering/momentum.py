"""Creation of momentum features"""

import pyspark.pandas as ps

from src.general.pkgs.utilities.helper import _validate_keys, load_obj


def create_momentum_features(team_spine, params) -> ps.DataFrame:

    spark_team_spine = team_spine.to_spark()
    sorted_spine = spark_team_spine.orderBy(["team", "date"])
    for window in params:
        _validate_keys(
            window.keys(),
            [
                "order_by",
                "aggregation_columns",
                "aggregation_type",
                "aggregation_window",
                "suffix",
            ],
        )

    sorted_spine

    test.groupby(window["order_by"])[
        "full_time_team_goals", "half_time_team_goals"
    ].transform(rolling)

