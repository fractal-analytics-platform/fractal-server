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

paths_12 = sorted(list(paths_12))
paths_13 = sorted(list(paths_13))

debug(paths_12)
debug(paths_13)

endpoints_12 = []
endpoints_13 = []
for path in paths_12:
    for method in api12["paths"][path].keys():
        endpoints_12.append(f"{method.upper()} {path}")
for path in paths_13:
    for method in api13["paths"][path].keys():
        endpoints_13.append(f"{method.upper()} {path}")
endpoints_12 = sorted(endpoints_12)
endpoints_13 = sorted(endpoints_13)

assert len(endpoints_12) == len(endpoints_13)
with open("raw_endpoints.txt", "w") as f:
    for ind, endpoint_12 in enumerate(endpoints_12):
        endpoint_13 = endpoints_13[ind]
        f.write(f"OLD {endpoint_12}\n")
        f.write(f"NEW {endpoint_13}\n")
        f.write("\n")
