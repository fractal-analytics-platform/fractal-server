import smtplib
from email.message import EmailMessage
from email.utils import formataddr

from fractal_server.config import MailSettings


def mail_new_oauth_signup(msg: str, mail_settings: MailSettings):
    """
    Send an email using the specified settings to notify a new OAuth signup.
    """

    mail_msg = EmailMessage()
    mail_msg.set_content(msg)
    mail_msg["From"] = formataddr((mail_settings.sender, mail_settings.sender))
    mail_msg["To"] = ", ".join(
        [
            formataddr((recipient, recipient))
            for recipient in mail_settings.recipients
        ]
    )
    mail_msg[
        "Subject"
    ] = f"[Fractal, {mail_settings.instance_name}] New OAuth signup"

    with smtplib.SMTP(mail_settings.smtp_server, mail_settings.port) as server:
        server.ehlo()
        if mail_settings.use_starttls:
            server.starttls()
            server.ehlo()

        server.login(
            user=mail_settings.sender, password=mail_settings.password
        )
        server.sendmail(
            from_addr=mail_settings.sender,
            to_addrs=mail_settings.recipients,
            msg=mail_msg.as_string(),
        )
