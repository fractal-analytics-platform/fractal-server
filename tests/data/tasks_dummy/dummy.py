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
This module only contains a dummy task, to be used in tests of fractal-server
"""
import json
import logging
import os
import time
from datetime import datetime
from datetime import timezone
from json.decoder import JSONDecodeError
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


def dummy(
    *,
    input_paths: list[str],
    output_path: str,
    metadata: Optional[Dict[str, Any]] = None,
    # arguments of this task
    message: str,
    index: int = 0,
    raise_error: bool = False,
    sleep_time: int = 0,
) -> Dict[str, Any]:
    """
    Dummy task

    This task appends to a json file the parameters it was called with, such
    that it is easy to parse the file in a test settings.

    Incidentally, this task defines the reference interface of a task.

    Arguments:
        input_paths:
            The paths to fetch data from
        output_path:
            The output path, pointing either to a file or to a directory in
            which the task will write its output files.
        metadata:
            Optional metadata about the input the task may need

        message:
            A message to be printed in the output file or in the raised error
        index: TBD
        raise_error:
            If `True`, raise an error
        sleep_time:
            Interval (in seconds) to be waited with a `time.sleep` statement

    Raises:
        ValueError: If `raise_error` is `True`

    Returns:
        metadata_update:
            A dictionary that will update the metadata
    """
    logger.info("[dummy] ENTERING")
    logger.info(f"[dummy] {input_paths=}")
    logger.info(f"[dummy] {output_path=}")

    if raise_error:
        raise ValueError(message)

    payload = dict(
        task="DUMMY TASK",
        timestamp=datetime.now(timezone.utc).isoformat(),
        input_paths=input_paths,
        output_path=output_path,
        metadata=metadata,
        message=message,
    )

    # Create output folder and set output file path
    if not os.path.isdir(output_path):
        os.makedirs(output_path, exist_ok=True)
    filename_out = f"{index}.result.json"
    out_fullpath = Path(output_path) / filename_out

    try:
        with out_fullpath.open("r") as fin:
            data = json.load(fin)
    except (JSONDecodeError, FileNotFoundError):
        data = []
    data.append(payload)
    with out_fullpath.open("w") as fout:
        json.dump(data, fout, indent=2)

    # Sleep
    logger.info(f"[dummy] Now starting {sleep_time}-seconds sleep")
    time.sleep(sleep_time)

    # Update metadata
    metadata_update = dict(dummy=f"dummy {index}", index=["0", "1", "2"])

    logger.info("[dummy] EXITING")

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
        message: str
        index: int = 0
        raise_error: bool = False
        sleep_time: int = 0

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
    metadata_update = dummy(**task_args.dict())

    if args.metadata_out:
        with open(args.metadata_out, "w") as fout:
            json.dump(metadata_update, fout)
    else:
        stdout.write(json.dumps(metadata_update))

    exit(0)
