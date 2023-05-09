import json

from devtools import debug  # noqa

with open("openapi_1_2_5.json", "r") as f:
    api12 = json.load(f)
with open("openapi_1_3_0.json", "r") as f:
    api13 = json.load(f)

paths_12 = set(api12["paths"].keys())
paths_13 = set(api13["paths"].keys())

intersection = set.intersection(paths_12, paths_13)
while intersection:
    element = intersection.pop()
    paths_12.remove(element)
    paths_13.remove(element)

debug(sorted(paths_12))
debug(sorted(paths_13))

for path in paths_12:
    assert path in api12["paths"].keys()
    val = api12["paths"][path]
    assert val
