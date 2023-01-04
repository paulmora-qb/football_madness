"""Creation of momentum features"""

import re

from pyspark.sql import DataFrame

from general.nodes.feature_engineering.create_columns import create_column_from_config


def create_momentum_features(team_spine, momentum_feature_params) -> DataFrame:

    core_columns = ["team", "date", "league", "home_away_indication"]

    df = team_spine.transform(
        lambda x: create_column_from_config(x, momentum_feature_params)
    )

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
