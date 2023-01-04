"""Function to redirect feature creation and initialize objects"""

from general.functions.feature_engineering import create_columns
from utilities.objects import initializes_objects


@initializes_objects
def create_column_from_config(*args, **kwargs):
    return create_columns.create_column_from_config(*args, **kwargs)
