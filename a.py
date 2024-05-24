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
from threading import Thread

from broker import Order_details,Socket_handling
from execuations import OrderExecuation
# Socket_handling(F.kotak_neo,broker_session).start_socket()

from checking import Checking

def a ():
    Socket_handling(F.kotak_neo,broker_session).start_socket()

start_socket_thread = Thread(name='socket_thread',target=a) 
start_socket_thread.start()

print(start_socket_thread.is_alive())

# import threading
# import time

# def example_thread():
#     time.sleep(5)

# # Create a thread
# thread = threading.Thread(target=example_thread)

# # Start the thread
# thread.start()

# # Check if the thread is alive
# print("Thread is alive:", thread.is_alive())

# # Wait for the thread to complete
# thread.join()

# # Check if the thread is alive again
# print("Thread is alive:", thread.is_alive())

