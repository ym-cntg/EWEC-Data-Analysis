"""UC2 Solar forecasting: Kedro-style pipelines (plain Python) for Databricks.

Layout:
    framework.py        Node / Pipeline / DataCatalog / runner (mini-Kedro)
    config.py           loads conf/parameters.yml and conf/catalog.yml
    spark_utils.py      time, sun-position and as-of-join helpers
    pipelines/<name>/   nodes.py (pure functions) + pipeline.py (wiring)
    registry.py         name -> pipeline
"""
__version__ = "0.1.0"
