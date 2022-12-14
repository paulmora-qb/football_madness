"""Model imputation"""

from general.functions.modeling import model_imputation
from utilities.objects import initializes_objects


@initializes_objects
def imputer(*args, **kwargs):
    return model_imputation.imputer(*args, **kwargs)
