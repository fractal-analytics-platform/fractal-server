import smtplib
from email.message import EmailMessage
from email.utils import formataddr

from cryptography.fernet import Fernet

from fractal_server.config import MailSettings


def mail_new_oauth_signup(msg: str, email_settings: MailSettings):
    """
    Send an email using the specified settings to notify a new OAuth signup.
    """

    mail_msg = EmailMessage()
    mail_msg.set_content(msg)
    mail_msg["From"] = formataddr(
        (email_settings.sender, email_settings.sender)
    )
    mail_msg["To"] = ", ".join(
        [
            formataddr((recipient, recipient))
            for recipient in email_settings.recipients
        ]
    )
    mail_msg[
        "Subject"
    ] = f"[Fractal, {email_settings.instance_name}] New OAuth signup"

    with smtplib.SMTP(
        email_settings.smtp_server, email_settings.port
    ) as server:
        server.ehlo()
        if email_settings.use_starttls:
            server.starttls()
            server.ehlo()
        if email_settings.use_login:
            password = (
                Fernet(email_settings.encryption_key.get_secret_value())
                .decrypt(email_settings.encrypted_password.get_secret_value())
                .decode("utf-8")
            )
            server.login(user=email_settings.sender, password=password)
        server.sendmail(
            from_addr=email_settings.sender,
            to_addrs=email_settings.recipients,
            msg=mail_msg.as_string(),
        )
