from ...framework import Node, Pipeline
from . import nodes as n


def create_pipeline() -> Pipeline:
    return Pipeline([
        Node(n.build_model_input, ["feat_table", "params:model_training"], "model_input", "build_model_input"),
        Node(n.train_models, ["spark", "model_input", "params:model_training", "params:run", "params:product_cfg", "params:evaluation"],
             "predictions", "train_models"),
    ], name="model_training")
