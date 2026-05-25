import logging

import pytest

from fractal_server.app.security.signup_email import (
    send_fractal_email_or_log_failure,
)
from fractal_server.config._email import PublicEmailSettings


async def test_send_fractal_email_or_log_failure(caplog, monkeypatch):
    email_settings_public = PublicEmailSettings(
        sender="fractal@fractal.fractal",
        recipients=["test@example.org"],
        smtp_server="localhost",
        port=1234,
        instance_name="fractal",
        use_starttls=False,
        use_login=True,
    )

    # Mock the logger, so that `propagate=True`
    import fractal_server.app.security.signup_email

    _logger = logging.getLogger("some-logger")
    monkeypatch.setattr(
        fractal_server.app.security.signup_email, "logger", _logger
    )

    caplog.clear()
    send_fractal_email_or_log_failure(
        subject="subject",
        msg="msg",
        email_settings=None,
    )
    assert "Cannot send email" in caplog.text
    assert "email_settings=None" in caplog.text

    caplog.clear()
    send_fractal_email_or_log_failure(
        subject="subject", msg="msg", email_settings=email_settings_public
    )
    assert "Could not send self-registration email" in caplog.text
    # The error message depends on the OS. The following works on Mac and Linux
    assert "Connection refused" in caplog.text


def test_password_not_none():
    email_settings_public = PublicEmailSettings(
        sender="fractal@fractal.fractal",
        recipients=["test@example.org"],
        smtp_server="localhost",
        port=1234,
        instance_name="fractal",
        use_starttls=False,
        use_login=True,
        password="1234",
    )

    assert email_settings_public.password.get_secret_value() == "1234"

    email_settings_public.password = None
    with pytest.raises(RuntimeError):
        email_settings_public.password_not_none
