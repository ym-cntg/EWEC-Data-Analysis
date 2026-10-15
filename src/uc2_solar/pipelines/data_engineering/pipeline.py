from ...framework import Node, Pipeline
from . import nodes as n


def create_pipeline() -> Pipeline:
    return Pipeline([
        Node(n.clean_target, ["raw_pv_telemetry", "params:sites", "params:data_engineering", "params:run"], "int_target_5min", "clean_target"),
        Node(n.unpack_reuniwatt, ["raw_reuniwatt", "params:data_engineering", "params:run"], "int_reuniwatt_5min", "unpack_reuniwatt"),
        Node(n.flag_broken_runs, ["int_reuniwatt_5min", "params:sites", "params:data_engineering"], "int_reuniwatt_run_flags", "flag_broken_runs"),
        Node(n.resample_reuniwatt, ["int_reuniwatt_5min", "int_reuniwatt_run_flags", "params:product_cfg", "params:feature_engineering"], "pri_reuniwatt", "resample_reuniwatt"),
        Node(n.resample_target, ["int_target_5min", "params:product_cfg", "params:data_engineering"], "pri_target", "resample_target"),
    ], name="data_engineering")
