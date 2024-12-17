import smtplib
from email.message import EmailMessage

from fractal_server.config import MailSettings


def report_to_mail(msg: str, mail_settings: MailSettings):
    """
    Send a report via email using the specified settings.
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
            to_addrs=mail_settings.recipients.split(","),
            msg=msg.as_string(),
        )
