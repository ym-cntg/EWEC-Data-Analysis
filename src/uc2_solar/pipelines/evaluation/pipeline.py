from ...framework import Node, Pipeline
from . import nodes as n


def create_pipeline() -> Pipeline:
    return Pipeline([
        Node(n.evaluate, ["spark", "predictions", "params:evaluation", "params:run"], ["metrics_summary", "metrics_by_step", "report"], "evaluate"),
    ], name="evaluation")
