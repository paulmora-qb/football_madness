"""Creation of momentum features"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import DoubleType

from general.nodes.feature_engineering.create_columns import create_column_from_config


def create_momentum_features(team_spine, params) -> DataFrame:

    core_columns = ["team", "date", "league", "home_away_indication"]

    # Replace the indication whether a team or lost with the information of points
    translation_dict = {"win": 3, "draw": 1, "loss": 0}

    df = team_spine.transform(
        lambda x: create_column_from_config(x, params["horizontal_averages"])
    ).transform(lambda x: create_column_from_config(x, params["window_operations"]))

    # for param in momentum_feature_params:
    #     function_path = param.pop("function")
    #     func = load_obj(function_path)
    #     list_of_new_columns = func(**param)

    #     for col in list_of_new_columns:
    #         team_spine = team_spine.select("*", col)

    # # TODO: Rethink the selection of the features through that mechanism
    # regex = re.compile("ftr_window_.*")
    # feature_columns = core_columns + list(filter(regex.match, team_spine.columns))
    # return team_spine.select(feature_columns).na.drop()
    return 1

    from pyspark.sql.functions import col, count, isnan, when

    team_spine.select(
        [
            count(when(isnan(c) | col(c).isNull(), c)).alias(c)
            for c in params["horizontal_averages"][1]["aggregation_columns"]
        ]
    ).show()
