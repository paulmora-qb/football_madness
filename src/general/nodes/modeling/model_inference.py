"""Model prediction functions"""

from general.functions.modeling import model_inference
from utilities.objects import initializes_objects


@initializes_objects
def model_prediction(*args, **kwargs):
    return model_inference.model_prediction(*args, **kwargs)
