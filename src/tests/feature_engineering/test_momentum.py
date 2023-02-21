"""Pytests for the momentum feature engineering"""

from typing import Dict

from pyspark.sql import DataFrame

from feature_engineering.momentum import create_momentum_features


def test_create_momentum_features(
    team_spine: DataFrame,
    momentum_params: Dict[str, str],
    momentum_expected: DataFrame,
    helpers,
) -> None:

    df_actual = create_momentum_features(team_spine, momentum_params)
    helpers.assert_equality(df_actual, momentum_expected)
