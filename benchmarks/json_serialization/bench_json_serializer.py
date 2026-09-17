import json
import statistics
import time
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

from pydantic import TypeAdapter

ITEMS_PER_GROUP = 100
REPETITIONS = 30

LEAF_FACTORIES = [
    lambda i: i,
    lambda i: i / 3.0,
    lambda i: f"value_{i}",
    lambda i: i % 2 == 0,
    lambda i: None,
    lambda i: datetime(2020, 1, 1) + timedelta(seconds=i),
    lambda i: UUID(int=i),
    lambda i: Path("/tmp", f"dir_{i}", f"file_{i}.txt"),
]


def _make_leaf(i: int) -> Any:
    return LEAF_FACTORIES[i % len(LEAF_FACTORIES)](i)


# --- Serializer 1: plain json.dumps with a `default` hook -----------------


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Path):
            return obj.as_posix()
        elif isinstance(obj, UUID):
            return str(obj)
        return super().default(obj)


def json_dumps(obj: Any) -> str:
    return json.dumps(obj, cls=CustomEncoder, indent=2)


# --- Serializer 2: pydantic TypeAdapter(Any) -------------------------------


# Optimization: Initialize the TypeAdapter only once
_ANY_TYPE_ADAPTER = TypeAdapter(Any)


def type_adapter_dumps(obj: Any) -> str:
    return _ANY_TYPE_ADAPTER.dump_json(obj).decode()


# --- Test-data generation ---------------------------------------------------


def build_test_data(node_count: int) -> Any:
    """
    Deterministically build a dict of groups, alternating between dict-
    and list-shaped groups, each holding `ITEMS_PER_GROUP` leaves, until
    `node_count` nodes (containers + leaves, root included) exist.
    """

    root: dict[str, Any] = {}
    size = 1
    group_index = 0
    while True:
        group_index += 1
        group_key = f"group_{group_index}"
        if group_index % 2 == 0:
            root[group_key] = []
            size += 1
            for leaf_index in range(ITEMS_PER_GROUP):
                if size == node_count:
                    return root
                root[group_key].append(_make_leaf(leaf_index))
                size += 1
        else:
            root[group_key] = {}
            size += 1
            for leaf_index in range(ITEMS_PER_GROUP):
                if size >= node_count:
                    return root
                root[group_key][f"item_{leaf_index}"] = _make_leaf(leaf_index)
                size += 1

    return root


def count_nodes(obj: Any) -> int:
    if isinstance(obj, dict):
        return 1 + sum(count_nodes(v) for v in obj.values())
    if isinstance(obj, list):
        return 1 + sum(count_nodes(v) for v in obj)
    return 1


# --- Benchmark driver --------------------------------------------------------


def time_calls(fn, obj: Any, repetitions: int) -> list[float]:
    timings = []
    for _ in range(repetitions):
        start = time.perf_counter()
        fn(obj)
        stop = time.perf_counter()
        timings.append(stop - start)
    return timings


def report(name: str, timings: list[float]) -> float:
    mean = statistics.mean(timings)
    print(
        f"[{name:>11s}]: mean={mean * 1000:4.2f} ms  "
        f"median={statistics.median(timings) * 1000:4.2f} ms  "
        f"min={min(timings) * 1000:4.2f} ms  "
        f"max={max(timings) * 1000:4.2f} ms"
    )
    return mean


def main(target_node_count: int) -> None:
    data = build_test_data(target_node_count)
    n_nodes = count_nodes(data)
    print(f"Generated nested structure with {n_nodes} nodes.")

    # Sanity check: both serializers must agree on the encoded value.
    encoded_via_json = json.loads(json_dumps(data))
    encoded_via_pydantic = json.loads(type_adapter_dumps(data))
    if encoded_via_json != encoded_via_pydantic:
        raise RuntimeError("serializers disagree on output")

    json_timings = time_calls(json_dumps, data, REPETITIONS)
    adapter_timings = time_calls(type_adapter_dumps, data, REPETITIONS)

    json_mean = report("json.dumps", json_timings)
    adapter_mean = report("TypeAdapter", adapter_timings)

    ratio = json_mean / adapter_mean
    print(f"TypeAdapter speed-up ratio: {ratio:.2f}x")
    print()


if __name__ == "__main__":
    for target_node_count in [100, 1_000, 10_000, 100_000]:
        main(target_node_count)
