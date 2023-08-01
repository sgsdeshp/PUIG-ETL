import smtplib
import ssl
import os
from email.message import EmailMessage

# Define email sender and receiver
email_sender = 'error.notifications.script@gmail.com'
# Ref for app access https://www.youtube.com/watch?v=g_j6ILT-X0k
email_password = os.environ.get('notification_email_pass')
email_receiver = 'support@puremoto.co.uk'
print(email_password)


def send_email(subject, body):
    em = EmailMessage()
    em['From'] = email_sender
    em['To'] = email_receiver
    em['Subject'] = subject
    em.set_content(body)

    # Add SSL (layer of security)
    context = ssl.create_default_context()

    # Log in and send the email
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
        smtp.login(email_sender, email_password)
        smtp.sendmail(email_sender, email_receiver, em.as_string())
