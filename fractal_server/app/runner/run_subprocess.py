import shlex
import subprocess  # nosec
from typing import Optional

from fractal_server.logger import get_logger
from fractal_server.string_tools import validate_cmd


def run_subprocess(
    cmd: str,
    allow_char: Optional[str] = None,
    logger_name: Optional[str] = None,
) -> subprocess.CompletedProcess:
    validate_cmd(cmd, allow_char=allow_char)
    logger = get_logger(logger_name)
    try:
        res = subprocess.run(  # nosec
            shlex.split(cmd), check=True, capture_output=True, encoding="utf-8"
        )
        return res
    except subprocess.CalledProcessError as e:
        logger.debug(
            f"Command '{e.cmd}' returned non-zero exit status {e.returncode}."
        )
        logger.debug(f"stdout: {e.stdout}")
        logger.debug(f"stderr: {e.stderr}")
        raise e
    except Exception as e:
        logger.debug(f"An error occurred while running command: {cmd}")
        logger.debug(str(e))
        raise e
