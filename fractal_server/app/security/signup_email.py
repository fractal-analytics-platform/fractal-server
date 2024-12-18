import smtplib
from email.message import EmailMessage

from fractal_server.config import MailSettings


def mail_new_oauth_signup(msg: str, mail_settings: MailSettings):
    """
    Send an email using the specified settings to notify a new OAuth signup.
    """

    msg = EmailMessage()
    msg.set_content(msg)
    msg["From"] = mail_settings.sender
    msg["To"] = ",".join(mail_settings.recipients)
    msg["Subject"] = f"Fractal {mail_settings.instance_name}: New Signup"

    with smtplib.SMTP(mail_settings.smtp_server, mail_settings.port) as server:
        server.ehlo()
        if mail_settings.use_tls:
            server.starttls()
            server.ehlo()

        server.login(
            user=mail_settings.sender,
            password=mail_settings.password,
            initial_response_ok=True,
        )
        server.sendmail(
            from_addr=mail_settings.sender,
            to_addrs=mail_settings.recipients,
            msg=msg.as_string(),
        )
