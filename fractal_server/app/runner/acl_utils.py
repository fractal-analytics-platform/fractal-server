from pathlib import Path

from ...config import get_settings
from ...syringe import Inject


def mkdir_with_acl(folder: Path):
    """ """

    settings = Inject(get_settings)
    FRACTAL_ACL_OPTIONS = settings.FRACTAL_ACL_OPTIONS
    print(FRACTAL_ACL_OPTIONS)

    if not folder.exists():
        folder.mkdir(parents=True, mode=0o777)
