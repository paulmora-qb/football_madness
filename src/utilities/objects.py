"""Augmentation of parameters"""

import importlib
from turtle import update
from typing import Callable, Tuple

OBJECT_KW = "object"


def _load_obj(object_path: str) -> Callable:

    object_path, object_name = object_path.rsplit(".", 1)
    module_object = importlib.import_module(object_path)

    if not hasattr(module_object, object_name):
        raise AttributeError(
            f"The object {object_path} does not have function {object_name}"
        )
    return getattr(module_object, object_name)


def _initiate_object(param):

    object_path = param.pop(OBJECT_KW)

    new_param_dict = {}
    for key, value in param.items():

        if isinstance(value, dict):

            if OBJECT_KW in value.keys():
                new_param_dict[key] = _initiate_object(value)
            else:
                new_param_dict[key] = _parse_for_objects(value)
        else:
            new_param_dict[key] = _parse_for_objects(value)

    return _load_obj(object_path)(**new_param_dict)


def _parse_for_objects(param):

    if isinstance(param, dict):

        if OBJECT_KW in param.keys():
            return _initiate_object(param)

        updated_dict = {}
        for key, value in param.items():

            if isinstance(value, dict):

                if OBJECT_KW in value.keys():
                    updated_dict[key] = _initiate_object(value)
                else:
                    updated_dict[key] = _parse_for_objects(value)

            else:
                updated_dict[key] = _parse_for_objects(value)
        return updated_dict

    if isinstance(param, (Tuple, list)):
        return [_parse_for_objects(item) for item in param]
    return param


def _initializes_objects(*args, **kwargs):

    adj_args = _parse_for_objects(args)
    adj_kwargs = _parse_for_objects(kwargs)
    return adj_args, adj_kwargs


def initializes_objects(func):
    def wrapper(*args, **kwargs):
        args, kwargs = _initializes_objects(*args, **kwargs)
        return func(*args, **kwargs)

    return wrapper
