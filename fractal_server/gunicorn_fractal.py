import logging

from gunicorn.glogging import Logger as GunicornLogger

logger = logging.getLogger("uvicorn.error")


class FractalGunicornLogger(GunicornLogger):
    error_fmt = r"%(asctime)s   - gunicorn.error - %(levelname)s - [pid %(process)d] - %(message)s"  # noqa: E501
    access_fmt = r"%(asctime)s  - gunicorn.access - %(levelname)s - [pid %(process)d] - %(message)s"  # noqa: E501
    datefmt = r"%Y-%m-%d %H:%M:%S,%u"
