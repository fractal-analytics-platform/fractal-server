"""
Script used to generate
- `FRACTAL_EMAIL_SETTINGS`
- `FRACTAL_EMAIL_SETTINGS_KEY`
used by `.github/workflows/oauth.yaml`.
"""
import json

from cryptography.fernet import Fernet

SENDER_CREDENTIAL = ("sender@localhost", "fakepassword")


def generate_email_settings():
    key = Fernet.generate_key().decode("utf-8")
    fractal_mail_settings = json.dumps(
        dict(
            sender=SENDER_CREDENTIAL[0],
            password=SENDER_CREDENTIAL[1],
            smtp_server="localhost",
            port=2525,
            instance_name="test",
            use_tls=False,
        )
    ).encode("utf-8")
    enc_fractal_mail_settings = (
        Fernet(key).encrypt(fractal_mail_settings).decode("utf-8")
    )

    print(f"FRACTAL_EMAIL_SETTINGS: {enc_fractal_mail_settings}")
    print(f"FRACTAL_EMAIL_SETTINGS_KEY: {key}")


if __name__ == "__main__":
    generate_email_settings()
