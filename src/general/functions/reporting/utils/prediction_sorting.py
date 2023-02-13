"""Creating standing table"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType

from general.functions.feature_engineering.spine import create_team_spine


def create_standing_table(data: DataFrame) -> DataFrame:

    true_and_pred_column = ["full_time_result", "tgt_full_time_result_pred"]

    team_data = create_team_spine(
        match_data=data.select(
            ["home_team", "away_team", "date",] + true_and_pred_column
        ),
        target_column=true_and_pred_column,
    )
    win_lose_dict = {"win": "3", "loss": "0", "draw": "1"}
    for col in true_and_pred_column:
        team_data = team_data.replace(win_lose_dict, subset=col)
        team_data = team_data.withColumn(col, f.col(col).cast(IntegerType()))

    team_data.groupBy("team").sum(*true_and_pred_column).show()
