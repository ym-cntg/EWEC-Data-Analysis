"""Load parameters and catalog from the conf/ directory, with run-time overrides."""
from copy import deepcopy
from datetime import date, timedelta
from pathlib import Path

try:
    import yaml
except ImportError as e:  # pragma: no cover
    raise ImportError("PyYAML is required: run `%pip install pyyaml` in the notebook") from e


def _deep_update(base: dict, overrides: dict) -> dict:
    for k, v in (overrides or {}).items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_update(base[k], v)
        else:
            base[k] = v
    return base


def load_config(conf_dir, run_overrides: dict = None):
    """Returns (params, catalog_entries). `run_overrides` updates the `run` section (e.g. from notebook widgets)."""
    conf_dir = Path(conf_dir)
    params = yaml.safe_load((conf_dir / "parameters.yml").read_text())
    catalog = yaml.safe_load((conf_dir / "catalog.yml").read_text())
    params = _deep_update(deepcopy(params), {"run": {k: v for k, v in (run_overrides or {}).items() if v not in (None, "")}})
    if not params["run"].get("end_date"):
        params["run"]["end_date"] = str(date.today() - timedelta(days=1))
    product = params["run"]["product"]
    if product not in params["products"]:
        raise ValueError(f"Unknown product '{product}'. Options: {list(params['products'])}")
    params["product_cfg"] = params["products"][product]

    fmt = {"output_schema": params["run"]["output_schema"], "product": product}
    resolved = {}
    for name, entry in catalog.items():
        e = dict(entry)
        for key in ("table", "path"):
            if key in e:
                e[key] = e[key].format(**fmt)
        resolved[name] = e
    return params, resolved
