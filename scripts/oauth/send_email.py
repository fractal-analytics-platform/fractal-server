import smtplib

recipients = "recipient@fractal.xy"
sender = "sender@localhost"
password = "fakepassword"  # nosec
with smtplib.SMTP("localhost", 2525) as server:
    server.set_debuglevel(1)
    server.ehlo()
    # server.starttls() # FIXME This fails on mailhog
    server.login(
        user=sender,
        password=password,
    )
    for recipient in recipients:
        server.sendmail(
            from_addr=sender,
            to_addrs=recipient,
            msg="User 'x@y.z' just registered.",
        )
