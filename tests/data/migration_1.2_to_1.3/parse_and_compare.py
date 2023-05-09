import json

from devtools import debug  # noqa


def _sort_function(string):
    """
    This is a brute-force function to print the 1.2 and 1.3 endpoint in the
    same order.
    """
    base = ["POST", "GET", "PATCH", "DELETE"].index(string.split()[0])
    if "download" in string:
        return base + 0
    elif "job_id" in string:
        return base + 100
    elif "job" in string:
        return base + 200
    elif "resource_id" in string and "resource_id" not in string:
        return base + 300
    elif "resource_id" in string and "resource_id" not in string:
        return base + 400
    elif "dataset_id" in string:
        return base + 500
    elif "workflow_task_id" in string:
        return base + 600
    elif (
        "wftask" in string
        or "add-task" in string
        or "rm-task" in string
        or "edit-task" in string
    ):  # noqa
        return base + 700
    elif "apply" in string:
        return base + 800
    elif "workflow" in string:
        return base + 900
    else:
        return base + 1000


def extract_info(api, paths):
    _endpoints = []
    _queryparams = {}
    for path in paths:
        for method in api["paths"][path].keys():
            method_path = f"{method.upper()} {path}"
            _endpoints.append(method_path)
            try:
                for par in api["paths"][path][method]["parameters"]:
                    if par["in"] == "query":
                        _queryparams[method_path] = _queryparams.get(
                            method_path, []
                        ) + [
                            par["name"]
                        ]  # noqa
            except KeyError:
                pass
    _endpoints = sorted(_endpoints, key=_sort_function)
    return _endpoints, _queryparams


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

endpoints_12, queryparams_12 = extract_info(api12, paths_12)
endpoints_13, queryparams_13 = extract_info(api13, paths_13)

debug(queryparams_12)
debug(queryparams_13)

assert len(endpoints_12) == len(endpoints_13)
with open("endpoints.txt", "w") as f:
    for ind, endpoint_12 in enumerate(endpoints_12):
        endpoint_13 = endpoints_13[ind]
        queryparam_12 = queryparams_12.get(endpoint_12)
        queryparam_13 = queryparams_13.get(endpoint_13)
        if queryparam_12:
            query_string_12 = f"  QUERY PARAMETERS: {queryparam_12}"
        else:
            query_string_12 = ""
        if queryparam_13:
            query_string_13 = f"  QUERY PARAMETERS: {queryparam_13}"
        else:
            query_string_13 = ""
        f.write(f"OLD {endpoint_12}{query_string_12}\n")
        f.write(f"NEW {endpoint_13}{query_string_13}\n")
        f.write("\n")
