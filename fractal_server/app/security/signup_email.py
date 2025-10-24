import smtplib
from email.message import EmailMessage
from email.utils import formataddr

from fractal_server.config import PublicEmailSettings
from fractal_server.logger import set_logger

logger = set_logger(__name__)


def send_fractal_email_or_log_failure(
    *,
    subject: str,
    msg: str,
    email_settings: PublicEmailSettings | None,
):
    """
    Send an email using the specified settings, or log about failure.
    """

    if email_settings is None:
        logger.error(
            f"Cannot send email with {subject=}, because {email_settings=}."
        )

    try:
        logger.info(f"START sending email with {subject=}.")
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
        ] = f"[Fractal, {email_settings.instance_name}] {subject}"
        with smtplib.SMTP(
            email_settings.smtp_server,
            email_settings.port,
        ) as server:
            server.ehlo()
            if email_settings.use_starttls:
                server.starttls()
                server.ehlo()
            if email_settings.use_login:
                server.login(
                    user=email_settings.sender,
                    password=email_settings.password.get_secret_value(),
                )
            server.sendmail(
                from_addr=email_settings.sender,
                to_addrs=email_settings.recipients,
                msg=mail_msg.as_string(),
            )
        logger.info(f"END sending email with {subject=}.")

    except Exception as e:
        logger.error(
            "Could not send self-registration email, "
            f"original error: {str(e)}."
        )
