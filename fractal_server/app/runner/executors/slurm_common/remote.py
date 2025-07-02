import argparse
import json
import os
import sys

from ..call_command_wrapper import call_command_wrapper
from fractal_server import __VERSION__


class FractalVersionMismatch(RuntimeError):
    """
    Custom exception for version mismatch
    """

    pass


def worker(
    *,
    in_fname: str,
    out_fname: str,
) -> None:
    """
    Execute a job, possibly on a remote node.

    Arguments:
        in_fname: Absolute path to the input file (must be readable).
        out_fname: Absolute path of the output file (must be writeable).
    """

    # Create output folder, if missing
    out_dir = os.path.dirname(out_fname)
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)

    # Execute the job and capture exceptions
    try:
        with open(in_fname) as f:
            input_data = json.load(f)

        # Fractal-server version must be identical
        server_fractal_server_version = input_data["fractal_server_version"]
        worker_fractal_server_version = __VERSION__
        if worker_fractal_server_version != server_fractal_server_version:
            raise FractalVersionMismatch(
                f"{server_fractal_server_version=} but "
                f"{worker_fractal_server_version=}"
            )

        # Get `worker_python_version` as a `list` since this is the type of
        # `server_python_version` after a JSON dump/load round trip.
        worker_python_version = list(sys.version_info[:3])

        # Print a warning for Python version mismatch
        server_python_version = input_data["python_version"]
        if worker_python_version != server_python_version:
            if worker_python_version[:2] != server_python_version[:2]:
                print(
                    "WARNING: "
                    f"{server_python_version=} but {worker_python_version=}."
                )

        # Extract some useful paths
        metadiff_file_remote = input_data["metadiff_file_remote"]
        log_path = input_data["log_file_remote"]

        # Execute command
        full_command = input_data["full_command"]
        call_command_wrapper(cmd=full_command, log_path=log_path)

        try:
            with open(metadiff_file_remote) as f:
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
        help="Path of input JSON file",
        required=True,
    )
    parser.add_argument(
        "--output-file",
        type=str,
        help="Path of output JSON file",
        required=True,
    )
    parsed_args = parser.parse_args()

    kwargs = dict(
        in_fname=parsed_args.input_file,
        out_fname=parsed_args.output_file,
    )
    worker(**kwargs)
