"""Creation of momentum features"""

import re

from pyspark.sql import DataFrame

from utilities.helper import load_obj


def create_momentum_features(team_spine, momentum_feature_params) -> DataFrame:

    core_columns = ["team", "season", "date", "league", "home_away_indication"]

    for param in momentum_feature_params:
        function_path = param.pop("function")
        func = load_obj(function_path)
        list_of_new_columns = func(**param)

        for col in list_of_new_columns:
            team_spine = team_spine.select("*", col)

    regex = re.compile("window_.*")
    feature_columns = core_columns + list(filter(regex.match, team_spine.columns))
    return team_spine.select(feature_columns).na.drop()
