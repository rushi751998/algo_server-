from .kotak_neo import Kotak_Neo_Login
from ..utils import load_cread, F


class SessionManager:
    sessions = {}
    login_cred_ = None

    @staticmethod
    def login_users():
        login_cred = load_cread()
        SessionManager.login_cred = login_cred
        for i in login_cred:
            if login_cred[i][F.broker_name] == F.kotak_neo:
                # print(login_cred[i])
                is_login, broker_session = Kotak_Neo_Login().login(login_cred[i])
                SessionManager.sessions[i] = {F.BROKER_NAME : login_cred[i][F.broker_name], F.SESSION:broker_session}
                
    @staticmethod                
    def get_session(user):
        return SessionManager.sessions[user][F.SESSION]
    
    @staticmethod
    def get_broker_name(user):
        return SessionManager.load_cread_[user]['broker_name']