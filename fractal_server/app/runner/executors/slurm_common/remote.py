"""
This module provides a simple self-standing script that executes arbitrary
python code received via pickled files on a cluster node.
"""
import argparse
import json
import logging
import os
import shutil
import subprocess  # nosec
import sys
from shlex import split

from fractal_server import __VERSION__
from fractal_server.app.runner.exceptions import TaskExecutionError
from fractal_server.string_tools import validate_cmd


class FractalVersionMismatch(RuntimeError):
    """
    Custom exception for version mismatch
    """

    pass


def _check_versions_mismatch(
    *,
    server_python_version: tuple[int, int, int],
    server_fractal_server_version: str,
):
    """
    Compare the server {python,cloudpickle,fractal_server} versions with the
    ones available to the current worker

    Arguments:
        server_versions:
            The version used in the fractal-server instance that created the
            cloudpickle file

    Raises:
        FractalVersionMismatch: If the cloudpickle or fractal_server versions
                                do not match with the ones on the server
    """

    worker_python_version = tuple(sys.version_info[:3])
    if worker_python_version != server_python_version:
        if worker_python_version[:2] != server_python_version[:2]:
            # FIXME: Turn this into an error, in some version post 2.14.
            logging.error(
                f"{server_python_version=} but {worker_python_version=}. "
                "This configuration will be deprecated in a future version, "
                "please contact the admin of this Fractal instance."
            )
        else:
            # Major.minor versions match, patch versions differ
            logging.warning(
                f"{server_python_version=} but {worker_python_version=}."
            )

    worker_fractal_server_version = __VERSION__
    if worker_fractal_server_version != server_fractal_server_version:
        raise FractalVersionMismatch(
            f"{server_fractal_server_version=} but "
            f"{worker_fractal_server_version=}"
        )


def _call_command_wrapper(cmd: str, log_path: str) -> None:
    """
    Call a command and write its stdout and stderr to files

    Raises:
        TaskExecutionError: If the `subprocess.run` call returns a positive
                            exit code
        JobExecutionError:  If the `subprocess.run` call returns a negative
                            exit code (e.g. due to the subprocess receiving a
                            TERM or KILL signal)
    """
    try:
        validate_cmd(cmd)
    except ValueError as e:
        raise TaskExecutionError(f"Invalid command. Original error: {str(e)}")

    # Verify that task command is executable
    if shutil.which(split(cmd)[0]) is None:
        msg = (
            f'Command "{split(cmd)[0]}" is not valid. '
            "Hint: make sure that it is executable."
        )
        raise TaskExecutionError(msg)

    with open(log_path, "w") as fp_log:
        try:
            result = subprocess.run(  # nosec
                split(cmd),
                stderr=fp_log,
                stdout=fp_log,
            )
        except Exception as e:
            raise e

    if result.returncode != 0:
        if os.path.isfile(log_path):
            with open(log_path, "r") as fp_stderr:
                err = fp_stderr.read()
        else:
            err = ""
        raise TaskExecutionError(
            f"Task failed with returncode={result.returncode}.\nSTDERR: {err}"
        )


def worker(
    *,
    in_fname: str,
    out_fname: str,
) -> None:
    """
    Execute a job, possibly on a remote node.

    Arguments:
        in_fname: Absolute path to the input pickle file (must be readable).
        out_fname: Absolute path of the output pickle file (must be writeable).
    """

    # Create output folder, if missing
    out_dir = os.path.dirname(out_fname)
    if not os.path.exists(out_dir):
        logging.debug(f"_slurm.remote.worker: create {out_dir=}")
        os.mkdir(out_dir)

    # Execute the job and capture exceptions
    try:
        with open(in_fname, "r") as f:
            input_data = json.load(f)

        server_python_version = input_data["python_version"]
        server_fractal_server_version = input_data["fractal_server_version"]

        _check_versions_mismatch(
            server_python_version=server_python_version,
            server_fractal_server_version=server_fractal_server_version,
        )

        # Extract some useful paths
        metadiff_file_remote = input_data["metadiff_file_remote"]
        log_path = input_data["log_file_remote"]

        # Execute command
        full_command = input_data["full_command"]
        _call_command_wrapper(cmd=full_command, log_path=log_path)

        try:
            with open(metadiff_file_remote, "r") as f:
                out_meta = json.load(f)
            result = (True, out_meta)
        except FileNotFoundError:
            # Command completed, but it produced no metadiff file
            result = (True, None)

    except Exception as e:
        # Exception objects are not serialisable. Here we save the relevant
        # exception contents in a serializable dictionary. Note that whenever
        # the task failed "properly", the exception is a `TaskExecutionError`
        # and it has additional attributes.
        import traceback

        exc_type, exc_value, traceback_obj = sys.exc_info()
        traceback_obj = traceback_obj.tb_next
        traceback_list = traceback.format_exception(
            exc_type,
            exc_value,
            traceback_obj,
        )
        traceback_string = "".join(traceback_list)
        exc_proxy = dict(
            exc_type_name=type(e).__name__,
            traceback_string=traceback_string,
        )
        result = (False, exc_proxy)

    # Write output file
    with open(out_fname, "w") as f:
        json.dump(result, f, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-file",
        type=str,
        help="Path of input pickle file",
        required=True,
    )
    parser.add_argument(
        "--output-file",
        type=str,
        help="Path of output pickle file",
        required=True,
    )
    parsed_args = parser.parse_args()
    logging.debug(f"{parsed_args=}")

    kwargs = dict(
        in_fname=parsed_args.input_file, out_fname=parsed_args.output_file
    )
    worker(**kwargs)
