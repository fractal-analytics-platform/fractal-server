import json
import statistics
import time
import uuid
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any

from pydantic import TypeAdapter

TARGET_NODE_COUNT = 10_000
ITEMS_PER_GROUP = 49
REPETITIONS = 20

LEAF_FACTORIES = [
    lambda i: i,
    lambda i: i / 3.0,
    lambda i: f"value_{i}",
    lambda i: i % 2 == 0,
    lambda i: None,
    lambda i: datetime(2020, 1, 1) + timedelta(seconds=i),
    lambda i: uuid.UUID(int=i),
    lambda i: Path("/tmp", f"dir_{i}", f"file_{i}.txt"),
]


def _make_leaf(i: int) -> Any:
    return LEAF_FACTORIES[i % len(LEAF_FACTORIES)](i)


# --- Serializer 1: plain json.dumps with a `default` hook -----------------


def _json_default(obj: Any) -> str:
    """
    Mirrors `fractal_server.app.models.base._json_default`.
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, Path):
        return obj.as_posix()
    elif isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(
        f"Object of type {type(obj).__name__} is not JSON serializable"
    )


def json_encoder_dumps(obj: Any) -> str:
    return json.dumps(obj, default=_json_default)


# --- Serializer 2: pydantic TypeAdapter(Any) -------------------------------

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
    created = 1
    leaf_index = 0
    group_index = 0

    while created < node_count:
        group: Any = [] if group_index % 2 else {}
        root[f"group_{group_index}"] = group
        created += 1

        for _ in range(ITEMS_PER_GROUP):
            if created >= node_count:
                break
            leaf = _make_leaf(leaf_index)
            leaf_index += 1
            if isinstance(group, list):
                group.append(leaf)
            else:
                group[f"item_{leaf_index}"] = leaf
            created += 1

        group_index += 1

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
        f"{name:>24s}: mean={mean * 1000:8.3f} ms  "
        f"median={statistics.median(timings) * 1000:8.3f} ms  "
        f"min={min(timings) * 1000:8.3f} ms  "
        f"max={max(timings) * 1000:8.3f} ms"
    )
    return mean


def main() -> None:
    data = build_test_data(TARGET_NODE_COUNT)
    n_nodes = count_nodes(data)
    print(f"Generated nested structure with {n_nodes} nodes.\n")

    # Sanity check: both serializers must agree on the encoded value.
    encoded_via_json = json.loads(json_encoder_dumps(data))
    encoded_via_pydantic = json.loads(type_adapter_dumps(data))
    if encoded_via_json != encoded_via_pydantic:
        raise RuntimeError("serializers disagree on output")

    print(f"Timing each serializer over {REPETITIONS} repetitions:\n")
    json_timings = time_calls(json_encoder_dumps, data, REPETITIONS)
    adapter_timings = time_calls(type_adapter_dumps, data, REPETITIONS)

    json_mean = report("json.dumps(default=...)", json_timings)
    adapter_mean = report("TypeAdapter(Any)", adapter_timings)

    ratio = json_mean / adapter_mean
    print(f"\njson.dumps / TypeAdapter time ratio: {ratio:.2f}x")


if __name__ == "__main__":
    main()
