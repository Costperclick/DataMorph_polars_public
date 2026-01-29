"""
Create a class by DataBase / SSH bridge storing your credentials.

Exemple :

class MyDataBase:
    URL = 'http://localhost'
    HOST = 'localhost'
    USERNAME = 'admin'
    PASSWORD = ''
    PORT = 22

class SSHCredentials :
    SSH_HOST = "whatever.this"
    SSH_PORT = 22
    SSH_USERNAME = "username"
    SSH_KEY_PATH = '/path/to/my/key'
    SSH_KEY_PASSWORD = 'supersecret'
"""


from dotenv import load_dotenv
import os

load_dotenv()


class MySpaceCredentials:
    def __init__(self):
        self.url = os.environ["MYSPACE_URL"]
        self.host = os.environ["MYSPACE_HOST"]
        self.username = os.environ["MYSPACE_USERNAME"]
        self.password = os.environ["MYSPACE_PASSWORD"]
        self.port = int(os.environ["MYSPACE_PORT"])
        self.name = os.environ["MYSPACE_NAME"]


class LisaCredentials:
    def __init__(self):
        self.url = os.environ["LISA_URL"]
        self.host = os.environ["LISA_HOST"]
        self.username = os.environ["LISA_USERNAME"]
        self.password = os.environ["LISA_PASSWORD"]
        self.port = int(os.environ["LISA_PORT"])
        self.name = os.environ["LISA_NAME"]


class AnalyticsCredentials:
    def __init__(self):
        self.url = os.environ["ANALYTICS_URL"]
        self.host = os.environ["ANALYTICS_HOST"]
        self.username = os.environ["ANALYTICS_USERNAME"]
        self.password = os.environ["ANALYTICS_PASSWORD"]
        self.port = int(os.environ["ANALYTICS_PORT"])
        self.name = os.environ["ANALYTICS_NAME"]


class SSHCredentials:
    def __init__(self):
        self.host = os.environ["SSH_HOST"]
        self.port = int((os.environ["SSH_PORT"]))
        self.username = os.environ["SSH_USERNAME"]
        self.key_path = os.environ["SSH_KEY_PATH"]
        self.key_password = os.environ["SSH_KEY_PASSWORD"]

