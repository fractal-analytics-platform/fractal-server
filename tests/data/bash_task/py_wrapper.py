import subprocess
from argparse import ArgumentParser
from pathlib import Path


parser = ArgumentParser()
parser.add_argument(
    "-j", "--json", help="Read parameters from json file", required=True
)
parser.add_argument(
    "--metadata-out",
    help="Output file to redirect serialised returned data",
    required=True,
)
args = parser.parse_args()
print(args)

subprocess.run(
    [Path(__file__).parent / "bash_task.sh", args.json, args.metadata_out]
)
