"""Pipeline registry: name -> Pipeline. `__default__` runs everything end to end."""
from .pipelines.data_engineering.pipeline import create_pipeline as de
from .pipelines.evaluation.pipeline import create_pipeline as ev
from .pipelines.feature_engineering.pipeline import create_pipeline as fe
from .pipelines.model_training.pipeline import create_pipeline as mt


def register_pipelines():
    pipes = {"data_engineering": de(), "feature_engineering": fe(), "model_training": mt(), "evaluation": ev()}
    pipes["__default__"] = pipes["data_engineering"] + pipes["feature_engineering"] + pipes["model_training"] + pipes["evaluation"]
    return pipes
