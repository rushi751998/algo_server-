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



        
future_token,broker_session = Login().setup()
from threading import Thread

from broker import Order_details,Socket_handling,is_order_rejected_func
from execuations import OrderExecuation
# Socket_handling(F.kotak_neo,broker_session).start_socket()

from checking import Checking



order_details =  Order_details(broker_session,F.kotak_neo)
is_not_empty,all_orders,filled_order,pending_order = order_details.order_book()
all_positions,open_position,closed_position = order_details.position_book()


a = is_order_rejected_func('240530000174495',broker_session,F.kotak_neo)


print(a)