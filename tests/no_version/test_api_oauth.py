import pytest
from devtools import debug

from fractal_server.config import get_data_settings
from fractal_server.config import get_db_settings
from fractal_server.config import get_email_settings
from fractal_server.config import get_oauth_settings
from fractal_server.config import get_settings
from fractal_server.syringe import Inject


@pytest.mark.oauth
async def test_oauth():
    for f in [
        get_data_settings,
        get_db_settings,
        get_email_settings,
        get_oauth_settings,
        get_settings,
    ]:
        s = Inject(f)
        debug(s)
