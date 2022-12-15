"""Transformers for fitting and prediction"""

from general.functions.preprocessing import transformer
from utilities.objects import initializes_objects


@initializes_objects
def fit(*args, **kwargs):
    return transformer.fit(*args, **kwargs)


@initializes_objects
def transform(*args, **kwargs):
    return transformer.transform(*args, **kwargs)


@initializes_objects
def fit_transform(*args, **kwargs):
    return transformer.fit_transform(*args, **kwargs)
