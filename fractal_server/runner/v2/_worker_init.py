def _get_worker_init_lines(worker_init: str | None) -> list[str] | None:
    """ """
    if worker_init is None:
        return None
    else:
        return worker_init.split("\n")
