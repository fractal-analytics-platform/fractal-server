import json
import sys

args_json_file = sys.argv[2]
with open(args_json_file) as f:
    arguments = json.load(f)

if arguments["parameter"] == 1:
    sys.exit("Bad result")
