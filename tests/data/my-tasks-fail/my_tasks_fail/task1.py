"""
My task 1
"""
import json
from argparse import ArgumentParser
from typing import Sequence


def task1(
    *,
    input_paths: Sequence[str],
    output_path: str,
    message: str = "default message",
):
    """
    My task 1
    """

    with open(output_path, "w") as f:
        f.write(f"{input_paths=}\n")
        f.write(f"{output_path=}\n")
        f.write(f"{message=}\n")

    return {"I did": "nothing"}


if __name__ == "__main__":

    # Parse `-j` and `--metadata-out` arguments
    parser = ArgumentParser()
    parser.add_argument("-j", "--json", required=True)
    parser.add_argument("--metadata-out", required=True)
    args = parser.parse_args()

    # Read parameters dictionary
    with open(args.json, "r") as f:
        task_arguments = json.load(f)

    # Run task
    metadata_update = task1(**task_arguments)

    # Write output metadata to file
    with open(args.metadata_out, "w") as fout:
        json.dump(metadata_update, fout)
