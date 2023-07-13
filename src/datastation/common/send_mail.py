import os

# https://docs.python.org/3/library/email.html#module-email
# https://docs.python.org/3/library/email.examples.html

class SendMail:
    # def __init__(self):
    #     pass

    @staticmethod
    def send(to_email, subject, message_body, attachment):
        email_template = " -s '{0}' {1}<<EOF {2} \nEOF"
        if attachment is not None:
            os.system("mail" + " -a " + attachment + email_template.format(subject, to_email, message_body))
        else:
            os.system("mail" + email_template.format(subject, to_email, message_body))
