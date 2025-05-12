import json
import logging
import sys
from argparse import ArgumentParser
from collections.abc import Callable
from pathlib import Path


def run_fractal_task(task_function: Callable) -> None:
    # Parse command-line-interface arguments
    parser = ArgumentParser()
    parser.add_argument(
        "--args-json", help="JSON file with task arguments", required=True
    )
    parser.add_argument(
        "--out-json",
        help="JSON file for task output metadata.",
        required=True,
    )
    cli_args = parser.parse_args()

    # Preliminary check
    if Path(cli_args.out_json).exists():
        logging.error(f"Output file {cli_args.out_json} already exists, exit.")
        sys.exit(1)

    # Read parameters dictionary
    with open(cli_args.args_json) as f:
        task_args = json.load(f)

    # Run task
    logging.info(f"START {task_function.__name__} task")
    metadata_update = task_function(**task_args)
    logging.info(f"END {task_function.__name__} task")

    # Write output metadata to file
    if metadata_update not in [None, {}]:
        with open(cli_args.out_json, "w") as fout:
            json.dump(metadata_update, fout, indent=2)
