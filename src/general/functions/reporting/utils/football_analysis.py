"""Creating standing table"""

from typing import Dict, Tuple

import numpy as np
import pandas as pd
import scipy.stats as stats
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as f
from pyspark.sql.functions import when

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


def create_betting_analysis(
    prediction_data: DataFrame,
    match_data: DataFrame,
    betting_profit_params: Dict[str, str],
) -> pd.DataFrame:
    """This function creates an overview of how much profit the machine learning
    algorithm would be creating when being used. This is done in the following way:

    1. Select the relevant broker to get the quotas
    2. Calculate the profits for the case the model is right and the model is wrong
    3. Differentiate the result by the number of model certainty

    Args:
        prediction_data (DataFrame): Dataframe containing the information of the
            prediction, and the prediction probability
        match_data (DataFrame): Dataframe containing the information which teams played
            and what the quotas are of the game
        betting_profit_params (List[str]): Parameter containing the information
            which broker to use and which probabilities are used to differentiate the
            model

    Returns:
        pd.DataFrame: Dataframe containing the profit, probability and the number
            of games within each bracket
    """

    broker = betting_profit_params["broker"]
    probabilities = betting_profit_params["probabilities"]

    join_cols = ["home_team", "away_team", "date"]
    betting_cols = [x for x in match_data.columns if (x.split("_")[0] in broker)]

    data = (
        prediction_data.select(
            join_cols
            + ["tgt_full_time_result_pred", "tgt_full_time_result", "probability"]
        )
        .join(match_data.select(join_cols + betting_cols), on=join_cols, how="inner",)
        .withColumn("betting_input", f.lit(1))
        .withColumn("probability", f.array_max(vector_to_array(f.col("probability"))))
    )

    betting_profit_data = pd.DataFrame(
        columns=["total_profit", "number_observations", "probability_threshold"]
    )
    for probability in probabilities:

        tmp_betting_profit_data = (
            data.withColumn(
                "relevant_odd",
                (
                    when(
                        f.col("tgt_full_time_result_pred") == "H",
                        f.col(f"{broker}_home_odds"),
                    )
                    .when(
                        f.col("tgt_full_time_result_pred") == "A",
                        f.col(f"{broker}_away_odds"),
                    )
                    .otherwise(f.col(f"{broker}_draw_odds"))
                ),
            )
            .withColumn(
                "betting_profits",
                f.when(
                    f.col("tgt_full_time_result_pred") == f.col("tgt_full_time_result"),
                    f.col("betting_input") * f.col("relevant_odd")
                    - f.col("betting_input"),
                ).otherwise(f.col("betting_input") * (-1)),
            )
            .filter(f.col("probability") > probability)
            .select(
                f.sum("betting_profits").alias("total_profit"),
                f.sum("betting_input").alias("number_observations"),
            )
            .withColumn("probability_threshold", f.lit(probability))
            .toPandas()
        )
        betting_profit_data = betting_profit_data.append(tmp_betting_profit_data)

    return betting_profit_data
