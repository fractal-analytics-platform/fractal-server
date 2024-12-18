import smtplib

sender = "sender@localhost"
password = "fakepassword"  # nosec
recipient = "recipient@example.org"
with smtplib.SMTP("localhost", 2525) as server:
    server.set_debuglevel(1)
    server.ehlo()
    server.starttls()
    server.login(
        user=sender,
        password=password,
    )
    server.sendmail(
        from_addr=sender,
        to_addrs=recipient,
        msg="User 'x@y.z' just registered.",
    )
