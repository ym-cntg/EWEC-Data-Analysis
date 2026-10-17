from ...framework import Node, Pipeline
from . import nodes as n


def create_pipeline() -> Pipeline:
    return Pipeline([
        Node(n.reuniwatt_run_features, ["pri_reuniwatt", "params:sites", "params:feature_engineering", "params:product_cfg"],
             "feat_reuniwatt_runs", "reuniwatt_run_features"),
        Node(n.build_origins, ["spark", "params:product_cfg", "params:run", "params:sites", "params:data_engineering"],
             "feat_origins", "build_origins"),
        Node(n.assemble_features, ["feat_origins", "feat_reuniwatt_runs", "params:product_cfg", "params:feature_engineering",
                                   "params:sites", "params:data_engineering"], "features_reuniwatt", "assemble_features"),
        Node(n.add_actual_history, ["features_reuniwatt", "feat_origins", "pri_target", "feat_reuniwatt_runs", "params:product_cfg",
                                    "params:feature_engineering", "params:sites"], "features_with_history", "add_actual_history"),
        Node(n.add_extra_features, ["features_with_history", "params:product_cfg", "params:feature_engineering", "params:data_engineering"],
             "features_unlabelled", "add_extra_features"),
        Node(n.add_labels, ["features_unlabelled", "pri_target", "params:product_cfg"], "feat_table", "add_labels"),
    ], name="feature_engineering")
