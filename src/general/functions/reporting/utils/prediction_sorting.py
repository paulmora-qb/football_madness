"""Creating standing table"""

from typing import Tuple

import numpy as np
import scipy.stats as stats
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType

from general.functions.feature_engineering.spine import create_team_spine


def create_standing_table(data: DataFrame) -> Tuple[DataFrame, float]:
    """This function takes the predictions and true values for a certain season/time
    / league and

    1. Calculates how many points each team scored according to true values and the
        prediction
    2. Calculates the ranking for each team (so far there is no mechanism for teams
        that have the same number of points...)
    3. Calculates the kendall tau correlation which gives an indication how good
        our prediction ranking is

    Args:
        data (DataFrame): Dataframe containing the true and predicted outcome of the
            game in the match spine format, meaning with having a home and away
            team

    Returns:
        Tuple[DataFrame, float]: Two things are returned, for once the dataframe
            with the number of points and the ranking according to these points. The
            second thing that is returned is the kendall-tau correlation which
            gives an indication how similar the ranking are.
    """

    true_and_pred_renaming_dict = {
        "tgt_full_time_result_pred": "predicted_number_points",
        "full_time_result": "true_number_points",
    }
    for old_name, new_name in true_and_pred_renaming_dict.items():
        data = data.withColumnRenamed(old_name, new_name)
    true_and_pred_columns = list(true_and_pred_renaming_dict.values())

    team_data = create_team_spine(
        match_data=data.select(
            ["home_team", "away_team", "date",] + true_and_pred_columns
        ),
        target_column=true_and_pred_columns,
    )

    win_lose_dict = {"win": "3", "loss": "0", "draw": "1"}
    for col in true_and_pred_columns:
        team_data = team_data.replace(win_lose_dict, subset=col)
        team_data = team_data.withColumn(col, f.col(col).cast(IntegerType()))

    agg_team_data = team_data.groupBy("team").agg(
        *[f.sum(col).alias(col) for col in true_and_pred_columns]
    )

    rank_col_names = []
    for col in true_and_pred_columns:
        rank_col_name = f"{col.split('_')[0]}_rank"
        rank_col_names.append(rank_col_name)
        agg_team_data = agg_team_data.withColumn(
            rank_col_name, f.dense_rank().over(Window.orderBy(f.desc(col)))
        )

    rank_array = np.array(agg_team_data.select(rank_col_names).collect())
    correlation, _ = stats.kendalltau(rank_array[:, 0], rank_array[:, 1])

    return agg_team_data, correlation
