class Email:
    def __init__(self,subject,body,from_recipient):
        self.subject = subject
        self.body= body

        split = from_recipient.split('@')
        if split[1] != "gmail.com":
            raise Exception("Invalid domain")

        self._from_recipient = from_recipient




    def send(self,to):
        print(f"Sending email from {self._from_recipient} with recipient and sibject {self.subject}, body {self.body} and to {to}")


if __name__ == "__main__":
    greeting_email = Email("Welcome","testing oop","greeting@igmail.com")
    greeting_email.subject = "hello2"

    greeting_email.send("welcome to oop")
    greeting_email.send("testing Python")


