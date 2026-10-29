import pytest

from fractal_server.app.security.self_registration import (
    SelfRegistrationExceptionData,
)
from fractal_server.app.security.self_registration import (
    get_self_registration_error,
)


def test_SelfRegistrationExceptionData():
    data = SelfRegistrationExceptionData(error_msg="something", params=[])
    assert data.to_text() == "something"

    data = SelfRegistrationExceptionData(
        error_msg="Here is %s.",
        params=[{"text": "an url", "url": "https://example.org"}],
    )
    assert data.to_text() == "Here is [an url](https://example.org/)."
    assert isinstance(data.to_dict()["params"][0]["url"], str)

    data = SelfRegistrationExceptionData(
        error_msg="Here is %s.",
        params=[{"text": "an email", "email": "someone@example.org"}],
    )
    assert data.to_text() == "Here is [an email](someone@example.org)."
    assert isinstance(data.to_dict()["params"][0]["email"], str)

    with pytest.raises(ValueError):
        SelfRegistrationExceptionData(
            error_msg="Here is %s.",
            params=[],
        )

    with pytest.raises(ValueError):
        SelfRegistrationExceptionData(
            error_msg="Here is %s.",
            params=[
                {"text": "an url", "url": "https://example.org"},
                {"text": "an url", "url": "https://example.org"},
            ],
        )


def test_get_self_registration_error_data(override_settings_factory):
    _URL = "https://example.org"
    _EMAIL = "someone@example.org"

    override_settings_factory(
        FRACTAL_REGISTRATION_URL=None, FRACTAL_HELP_EMAIL=None
    )
    assert "contact an administrator" in get_self_registration_error().to_text()

    override_settings_factory(
        FRACTAL_REGISTRATION_URL=_URL, FRACTAL_HELP_EMAIL=None
    )
    assert (
        "follow the instructions at" in get_self_registration_error().to_text()
    )

    override_settings_factory(
        FRACTAL_REGISTRATION_URL=_URL, FRACTAL_HELP_EMAIL=_EMAIL
    )
    assert (
        "follow the instructions at" in get_self_registration_error().to_text()
    )

    override_settings_factory(
        FRACTAL_REGISTRATION_URL=None, FRACTAL_HELP_EMAIL=_EMAIL
    )
    assert "contact an admin at" in get_self_registration_error().to_text()
