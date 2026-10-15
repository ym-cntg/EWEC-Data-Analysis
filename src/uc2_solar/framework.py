"""Mini-Kedro: nodes, pipelines, a data catalog and a sequential runner.

- A Node wraps a pure function with named inputs and outputs.
- Inputs are catalog dataset names, `params:<dotted.path>` for parameters, or `spark` for the SparkSession.
- Outputs registered in the catalog are persisted (Delta table or text file); others are kept in memory.
"""
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Union


@dataclass
class Node:
    func: Callable
    inputs: Sequence[str]
    outputs: Union[str, Sequence[str], None]
    name: str

    @property
    def output_list(self) -> List[str]:
        if self.outputs is None:
            return []
        return [self.outputs] if isinstance(self.outputs, str) else list(self.outputs)


@dataclass
class Pipeline:
    nodes: List[Node]
    name: str = "pipeline"

    def __add__(self, other: "Pipeline") -> "Pipeline":
        return Pipeline(self.nodes + other.nodes, f"{self.name}+{other.name}")

    def describe(self) -> str:
        lines = [f"Pipeline '{self.name}' ({len(self.nodes)} nodes)"]
        for i, n in enumerate(self.nodes, 1):
            lines.append(f"  {i:>2}. {n.name}: {list(n.inputs)} -> {n.output_list}")
        return "\n".join(lines)


class DataCatalog:
    """Loads and saves datasets by name. Table datasets are Delta tables; text datasets are files."""

    def __init__(self, spark, entries: Dict[str, dict], base_dir: Optional[Path] = None):
        self.spark = spark
        self.entries = entries
        self.base_dir = Path(base_dir or ".")
        self._memory: Dict[str, object] = {}

    def __contains__(self, name: str) -> bool:
        return name in self.entries or name in self._memory

    def load(self, name: str):
        if name in self._memory:
            return self._memory[name]
        e = self.entries.get(name)
        if e is None:
            raise KeyError(f"Dataset '{name}' is neither in the catalog nor produced earlier in this run")
        if e["type"] == "table":
            return self.spark.table(e["table"])
        if e["type"] == "text":
            return (self.base_dir / e["path"]).read_text()
        raise ValueError(f"Unsupported dataset type {e['type']}")

    def save(self, name: str, data):
        e = self.entries.get(name)
        if e is None:
            self._memory[name] = data
            return data
        if e.get("read_only"):
            raise PermissionError(f"Dataset '{name}' is read-only")
        if e["type"] == "table":
            table = e["table"]
            schema = table.rsplit(".", 1)[0]
            if schema.split(".")[-1].lower() in {"bronze", "silver", "gold"}:
                raise PermissionError(f"Refusing to write pipeline output into shared schema '{schema}'")
            w = data.write.mode("overwrite").option("overwriteSchema", "true")
            if e.get("partition_by"):
                w = w.partitionBy(*e["partition_by"])
            w.saveAsTable(table)
            out = self.spark.table(table)
            print(f"    saved {name} -> {table}")
            self._memory[name] = out
            return out
        if e["type"] == "text":
            path = self.base_dir / e["path"]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(data)
            print(f"    saved {name} -> {path.resolve()}")
            self._memory[name] = data
            return data
        raise ValueError(f"Unsupported dataset type {e['type']}")


def _param(params: dict, dotted: str):
    v = params
    for part in dotted.split("."):
        v = v[part]
    return v


def run_pipeline(pipeline: Pipeline, catalog: DataCatalog, params: dict, from_node: str = None, to_node: str = None) -> Dict[str, float]:
    """Run nodes in order. `from_node` / `to_node` (names) restrict the range, e.g. to resume after a failure."""
    names = [n.name for n in pipeline.nodes]
    start = names.index(from_node) if from_node else 0
    stop = names.index(to_node) + 1 if to_node else len(names)
    timings = {}
    print(pipeline.describe())
    for node in pipeline.nodes[start:stop]:
        t0 = time.time()
        print(f"▶ {node.name}")
        args = []
        for inp in node.inputs:
            if inp == "spark":
                args.append(catalog.spark)
            elif inp.startswith("params:"):
                args.append(_param(params, inp[len("params:"):]))
            else:
                args.append(catalog.load(inp))
        try:
            result = node.func(*args)
        except Exception as e:
            import traceback
            msg = (f"FAILED at node '{node.name}' (pipeline '{pipeline.name}')\n"
                   f"Resume with from_node = {node.name}\n\n{type(e).__name__}: {e}\n\n{traceback.format_exc()}")
            err_path = catalog.base_dir / "results" / "last_error.txt"
            err_path.parent.mkdir(parents=True, exist_ok=True)
            err_path.write_text(msg)
            print(f"  ✗ FAILED at node '{node.name}': {type(e).__name__}: {str(e)[:500]}")
            print(f"    Full error saved to {err_path.resolve()}; commit & push it to share.")
            raise
        outs = node.output_list
        if len(outs) == 1:
            catalog.save(outs[0], result)
        elif outs:
            for o, r in zip(outs, result):
                catalog.save(o, r)
        timings[node.name] = round((time.time() - t0) / 60, 2)
        print(f"  ✓ {node.name} ({timings[node.name]} min)")
    return timings
