import neo_api_client
from .BaseLogin import BaseLogin
from server.utils import send_message
import pandas as pd
from  datetime import datetime as dt
from dateutil.relativedelta import relativedelta
import time
import pandas as pd
import threading
from server.utils import set_coloumn_name, order_staus_dict,F, env_variables as env
nan = 'nan'

option_chain = {}
ticker_to_token= {}

class Kotak_Neo_Login(BaseLogin):
    
    def __init__(self):
        BaseLogin.__init__(self)
        pass
    
    # @classmethod
    def login(self,credentials):
        is_login = False
        broker_session = None
        while not is_login :
            session_validation_key, broker_session = Kotak_Neo_Login.process(credentials)
            check_validation_key = credentials['session_validation_key']
            if session_validation_key == check_validation_key:
                is_login = True
                self.setBrokerHandle(credentials['id'],broker_session)
                # self.setAccessToken(session_validation_key)
                
            else : 
                time.sleep(5) 
                self.login()
            # except Exception as e :
            #     send_message(message = f'Not able to Login\nissue :{e}', emergency = True)
            #     time.sleep(5) 
        return is_login, broker_session
        
            
    # @classmethod
    def process(credentials):
        broker_session = neo_api_client.NeoAPI(consumer_key=credentials['consumer_key'],
                            consumer_secret=credentials['secretKey'], environment='prod',
                            access_token=None, neo_fin_key=None)
        broker_session.login(mobilenumber=credentials['mobileNumber'], password=credentials['login_password'])
        session = broker_session.session_2fa(credentials['two_factor_code'])
        return session[F.data]['greetingName'],broker_session 
            
        