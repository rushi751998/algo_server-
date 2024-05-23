# from neo_api_client import NeoAPI
import io
import neo_api_client
import threading
import os
from dotenv import load_dotenv, dotenv_values 
load_dotenv() 
from utils import get_db,env_variables
env_variables.load_env_variable()
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
from broker import Login
from telegram_bot import *
consumer_key = os.environ.get("consumer_key")
secretKey = os.environ.get("secretKey")
mobileNumber = os.environ.get("mobileNumber")
login_password = os.environ.get("login_password")
two_factor_code = os.environ.get("two_factor_code")
import threading
import time
from utils import Fields as F

from broker import Order_details,get_ltp

import pandas as pd

# def on_message(message):
#     print('[Res]: ', message)


# def on_error(message):
#     result = message
#     print('[OnError]: ', result)

# client = neo_api_client.NeoAPI(consumer_key=consumer_key,
#                                consumer_secret=secretKey, environment='prod',
#                                access_token=None, neo_fin_key=None)


# client.login(mobilenumber=mobileNumber, password=login_password)
# session = client.session_2fa(two_factor_code)
# client.on_message = on_message  # called when message is received from websocket
# client.on_error = on_error  # called when any error or exception occurs in code or websocket

# broker_name = F.kotak_neo
# print(get_ltp('41941',broker_name))

# 
# print(today)
# if :
#     print(True)

        
future_token,broker_session = Login().setup()

from broker import Order_details,Socket_handling
# Socket_handling(F.kotak_neo,broker_session).start_socket()

all_orders,filled_order,pending_order = Order_details(broker_session,F.kotak_neo).order_book()
all_orders.to_csv('order.csv')
market_execute_price = all_orders#[all_orders[F.order_id] == '240523000390928'].iloc[0]#[F.price]
print( f"Final market price : ", market_execute_price )




# print(pending_order)

    
            

