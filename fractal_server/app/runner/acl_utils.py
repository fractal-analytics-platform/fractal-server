from pathlib import Path

from ...config import get_settings
from ...syringe import Inject


def mkdir_with_acl(folder: Path):
    """
    TBD
    """

    if folder.exists():
        raise ValueError(f"{str(folder)} already exists.")

    settings = Inject(get_settings)
    FRACTAL_ACL_OPTIONS = settings.FRACTAL_ACL_OPTIONS

    if FRACTAL_ACL_OPTIONS == "none":
        folder.mkdir(parents=True)
    elif FRACTAL_ACL_OPTIONS == "standard":
        raise NotImplementedError()
    elif FRACTAL_ACL_OPTIONS == "nfs4":
        raise NotImplementedError()
    else:
        raise ValueError(f"{FRACTAL_ACL_OPTIONS=} not supported")

    # if not folder.exists():
    #    folder.mkdir(parents=True, mode=0o777)
