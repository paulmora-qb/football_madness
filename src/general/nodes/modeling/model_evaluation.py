"""Model Evaluation"""

from general.functions.modeling import model_evaluation
from utilities.objects import initializes_objects


@initializes_objects
def prediction_evaluation(*args, **kwargs):
    return model_evaluation.prediction_evaluation(*args, **kwargs)
