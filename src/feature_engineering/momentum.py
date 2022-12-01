"""Creation of momentum features"""

from pyspark.sql import DataFrame

from src.general.pkgs.utilities.helper import _validate_keys, load_obj


def create_momentum_features(team_spine, params) -> DataFrame:

    for param in params:
        _validate_keys(
            param.keys(),
            [
                "function",
                "partition_by",
                "order_by",
                "aggregation_columns",
                "aggregation_type",
                "aggregation_range",
                "suffix",
            ],
        )

        function_path = param.pop("function")
        func = load_obj(function_path)
        list_of_functions = func(**param)

    team_spine.withColumn("test", list_of_functions[0])

