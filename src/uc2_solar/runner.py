"""Entry point used by the Databricks notebooks: `run("feature_engineering", spark, repo_root, overrides)`."""
from pathlib import Path

from .config import load_config
from .framework import DataCatalog, run_pipeline
from .registry import register_pipelines


def run(pipeline: str, spark, repo_root, overrides: dict = None, base_dir=".", from_node: str = None, to_node: str = None):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    params, entries = load_config(Path(repo_root) / "conf", overrides)
    print(f"Run settings: {params['run']}")
    catalog = DataCatalog(spark, entries, base_dir)
    pipes = register_pipelines()
    if pipeline not in pipes:
        raise ValueError(f"Unknown pipeline '{pipeline}'. Options: {list(pipes)}")
    return run_pipeline(pipes[pipeline], catalog, params, from_node, to_node)
