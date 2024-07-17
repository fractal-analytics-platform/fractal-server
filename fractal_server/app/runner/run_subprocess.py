import shlex
import subprocess  # nosec
from typing import Optional

from fractal_server.logger import get_logger


def run_subprocess(
    cmd: str, logger_name: Optional[str] = None
) -> subprocess.CompletedProcess:
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
