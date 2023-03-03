# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
#
# Original authors:
# Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
# Tommaso Comparin <tommaso.comparin@exact-lab.it>
# Marco Franzon <marco.franzon@exact-lab.it>
#
# This file is part of Fractal and was originally developed by eXact lab S.r.l.
# <exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
# Institute for Biomedical Research and Pelkmans Lab from the University of
# Zurich.
"""
This module only contains a dummy task (to be executed in parallel over several
components), to be used in tests of fractal-server
"""
import json
import logging
import os
import time
from datetime import datetime
from datetime import timezone
from pathlib import Path
from sys import stdout
from typing import Any
from typing import Dict
from typing import Optional

from pydantic import BaseModel


logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s; %(levelname)s; %(message)s"
)
logger = logging.getLogger(__name__)


def dummy_parallel(
    *,
    input_paths: list[str],
    output_path: str,
    component: str,
    metadata: Optional[Dict[str, Any]] = None,
    # arguments of this task
    message: str,
    raise_error: bool = False,
    sleep_time: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Dummy task to be run in parallel

    This task writes its arguments to to a JSON file named `component`.json (in
    the `output_path` parent folder); mapping this task over a list of
    `component`s produces a corresponding list of files that can be parsed in
    tests.

    Arguments:
        input_paths:
            The paths to fetch data from
        output_path:
            The output path, pointing either to a file or to a directory in
            which the task will write its output files.
        component:
            The component to process, e.g. component="1"
        metadata:
            Optional metadata about the input the task may need
        message:
            A message to be printed in the output file or in the raised error
        raise_error:
            If `True`, raise an error
        sleep_time:
            Interval to sleep, in seconds.

    Raises:
        ValueError: If `raise_error` is `True`

    Returns:
        metadata_update:
            A dictionary that will update the metadata
    """
    logger.info("[dummy_parallel] ENTERING")
    logger.info(f"[dummy_parallel] {input_paths=}")
    logger.info(f"[dummy_parallel] {output_path=}")

    if raise_error:
        raise ValueError(message)

    payload = dict(
        task="DUMMY TASK",
        timestamp=datetime.now(timezone.utc).isoformat(),
        input_paths=input_paths,
        output_path=output_path,
        metadata=metadata,
        component=component,
        message=message,
    )

    if sleep_time:
        logger.info(f"[dummy_parallel] Now let's sleep {sleep_time=} seconds")
        time.sleep(sleep_time)

    # Create folder output and set output file path
    if not os.path.isdir(output_path):
        os.makedirs(output_path, exist_ok=True)
    safe_component = component.replace(" ", "_").replace("/", "_")
    safe_component = safe_component.replace(".", "_")
    out_fullpath = str(Path(output_path) / f"{safe_component}.result.json")

    # Write output
    with open(out_fullpath, "w") as fout:
        json.dump(payload, fout, indent=2, sort_keys=True)

    logger.info("[dummy_parallel] EXITING")

    # Return empty metadata, since the "history" will be filled by fractal
    metadata_update: Dict = {}
    return metadata_update


if __name__ == "__main__":
    from argparse import ArgumentParser

    class TaskArguments(BaseModel):
        """
        Wrap task arguments to ease marshalling

        This way we can automatically cast the input from command line onto
        the correct type required by the task.
        """

        input_paths: list[str]
        output_path: str
        metadata: Optional[Dict[str, Any]] = None
        component: str
        message: str
        raise_error: bool = False
        sleep_time: Optional[int] = None

    parser = ArgumentParser()
    parser.add_argument("-j", "--json", help="Read parameters from json file")
    parser.add_argument(
        "--metadata-out",
        help=(
            "Output file to redirect serialised returned data "
            "(default stdout)"
        ),
    )

    args = parser.parse_args()

    if args.metadata_out and Path(args.metadata_out).exists():
        logger.error(
            f"Output file {args.metadata_out} already exists. Terminating"
        )
        exit(1)

    pars = {}
    if args.json:
        with open(args.json, "r") as f:
            pars = json.load(f)

    task_args = TaskArguments(**pars)
    metadata_update = dummy_parallel(**task_args.dict())

    if args.metadata_out:
        with open(args.metadata_out, "w") as fout:
            json.dump(metadata_update, fout)
    else:
        stdout.write(json.dumps(metadata_update))

    exit(0)
