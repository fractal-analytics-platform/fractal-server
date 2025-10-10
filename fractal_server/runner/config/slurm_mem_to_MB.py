from fractal_server.logger import set_logger
from fractal_server.runner.exceptions import SlurmConfigError


logger = set_logger(__name__)


def slurm_mem_to_MB(raw_mem: str | int) -> int:
    """
    Convert a memory-specification string into an integer (in MB units), or
    simply return the input if it is already an integer.

    Supported units are `"M", "G", "T"`, with `"M"` being the default; some
    parsing examples are: `"10M" -> 10000`, `"3G" -> 3000000`.

    Args:
        raw_mem:
            A string (e.g. `"100M"`) or an integer (in MB).

    Returns:
        Integer value of memory in MB units.
    """

    info = f"[_parse_mem_value] {raw_mem=}"
    error_msg = (
        f"{info}, invalid specification of memory requirements "
        "(valid examples: 93, 71M, 93G, 71T)."
    )

    # Handle integer argument
    if type(raw_mem) is int:
        return raw_mem

    # Handle string argument
    if not raw_mem[0].isdigit():  # fail e.g. for raw_mem="M100"
        logger.error(error_msg)
        raise SlurmConfigError(error_msg)
    if raw_mem.isdigit():
        mem_MB = int(raw_mem)
    elif raw_mem.endswith("M"):
        stripped_raw_mem = raw_mem.strip("M")
        if not stripped_raw_mem.isdigit():
            logger.error(error_msg)
            raise SlurmConfigError(error_msg)
        mem_MB = int(stripped_raw_mem)
    elif raw_mem.endswith("G"):
        stripped_raw_mem = raw_mem.strip("G")
        if not stripped_raw_mem.isdigit():
            logger.error(error_msg)
            raise SlurmConfigError(error_msg)
        mem_MB = int(stripped_raw_mem) * 10**3
    elif raw_mem.endswith("T"):
        stripped_raw_mem = raw_mem.strip("T")
        if not stripped_raw_mem.isdigit():
            logger.error(error_msg)
            raise SlurmConfigError(error_msg)
        mem_MB = int(stripped_raw_mem) * 10**6
    else:
        logger.error(error_msg)
        raise SlurmConfigError(error_msg)

    logger.debug(f"{info}, return {mem_MB}")
    return mem_MB
