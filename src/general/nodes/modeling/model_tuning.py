"""Model tuning"""

from general.functions.modeling import model_tuning
from utilities.objects import initializes_objects


@initializes_objects
def tuner(*args, **kwargs):
    return model_tuning.tuner(*args, **kwargs)
