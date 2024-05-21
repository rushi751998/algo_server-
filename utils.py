import pymongo
import os
from  datetime import datetime as dt,timedelta
from dotenv import load_dotenv
from typing import Dict, List 
import requests
from bs4 import BeautifulSoup
from telegram_bot import logger_bot,emergency_bot
import ast
import datetime

kotak_transaction_type_dict= {
                            'Buy' : 'B',
                            'Sell' : 'S'
                        }

order_staus_dict = {
                    200 :	'Gets the Positoin data for a client account',
                    400	:   'Invalid or missing input parameters',
                    403 :	'Invalid session, please re-login to continue',
                    429 :	'Too many requests to the API',
                    500 :	'Unexpected error',
                    502 :	'Not able to communicate with OMS',
                    503 :	'Trade API service is unavailable',
                    504 :	'Gateway timeout, trade API is unreachable',    
                    # 5204 :  'Gateway timeout, trade API is unreachable'
                    }


def is_hoilyday() :
    try : 
        url = "https://zerodha.com/marketintel/holiday-calendar/"
        response = requests.get(url)
        hoilyday_dict = {5 : 'Saturday',6 : 'Sunday'}
        date = str(dt.today().date())
        weekday = dt.strptime(date, "%Y-%m-%d").weekday()
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            holiday_table = soup.find('table')
            
            if holiday_table:
                for row in holiday_table.find_all('tr')[1:]:  #
                    columns = row.find_all('td')
                    date_fetched = columns[1].text.strip()
                    # print(f'soup : {date_fetched}')
                    
                    date_fetched = dt.strptime(date_fetched, "%d %b %Y").date()
                    holiday_name = columns[2].text.strip()
                    hoilyday_dict[str(date_fetched)] = holiday_name
                
                # logger_bot(f'hoilyday_dict : {hoilyday_dict}')
                if (date in hoilyday_dict) or (weekday in hoilyday_dict):
                    reason = hoilyday_dict[date] if date in hoilyday_dict else hoilyday_dict[weekday]
                    return True, reason
                else:
                    return False, None

            else:
                emergency_bot("Holiday table not found on the page.")
                return False, False
    except Exception as e :     
        emergency_bot("Failed to retrieve NSE holiday data. Status code: {e}")
        return False, False
        
def is_market_time():
    current_time = dt.today().time()
    start_time = datetime.time(hour=9,minute=15,second=0)
    end_time = datetime.time(hour=15,minute=30,second=0)
    if (current_time > start_time) and (current_time < end_time):
        return True
    else : 
        return False
        
def get_ist_now():
    return dt.now() + timedelta(0)

def get_db(db_name='order_book'):
    mongo_db = pymongo.MongoClient(env_variables.mongodb_link)
    entry_id= mongo_db[db_name]
    return entry_id

def set_coloumn_name(df,broker_name):
    if  broker_name == 'kotak_neo'   :
        column_name_dict = {
            'nOrdNo':'order_id',
            'ordDtTm':'order_time',
            'trdSym':'order_symbol',
            'tok':'symbol_token',
            'qty':'qty',
            'fldQty':'filled_qty',
            'prc':'price',
            'trnsTp':'transaction_type',
            'prod' :'product',
            'exSeg':'exchange_segement',
            'ordSt':'order_status',
            'stkPrc':'strike_price',
            'optTp':'option_type',
            'brdLtQty':'brdLtQty',
            'expDt':'expiry_date',
            'GuiOrdId':'tag',
            'type' : 'type',
            'buyAmt'  : 'buy_amount',
            'flBuyQty' : 'filed_buy_qty',
            'flSellQty' : 'filed_sell_qty',
            'sellAmt' : 'sell_amount',
            
        }
    if   broker_name =='fyers':
        pass

        
    df.rename(columns = column_name_dict,inplace = True)
    df.to_csv('order.csv')
    return df


class env_variables:
    env_variable_initilised = False
    mongodb_link : str
    consumer_key : str
    secretKey : str
    mobileNumber : str
    login_password : str
    broker_name : str  
    session_validation_key : str
    two_factor_code : str
    allowed_loss_percent : str
    exceptational_hoilydays : list
    exceptational_tradingdays  : list

    login : str
    first_order : str
    second_order : str
    third_order : str
    fourth_order : str
    fifth_order : str
    exit_orders : str
    logout_session : str
    
    @classmethod
    def load_env_variable (self):
        import os
        load_dotenv() 
        
        self.env_variable_initilised= True
        
        self.mongodb_link =os.environ['mongodb_link'] 
        self.consumer_key =os.environ['consumer_key'] 
        self.secretKey =os.environ['secretKey'] 
        self.mobileNumber=os.environ['mobileNumber'] 
        self.login_password =os.environ['login_password'] 
        self.broker_name =os.environ['broker_name']    
        self.session_validation_key =os.environ['session_validation_key'] 
        self.two_factor_code =os.environ['two_factor_code'] 
        self.allowed_loss_percent =os.environ['allowed_loss_percent'] 
        # self.hoilydays =os.environ['hoilydays'] 
        self.exceptational_tradingdays =  ast.literal_eval(os.environ['exceptational_tradingdays'] )
        self.exceptational_hoilydays = ast.literal_eval(os.environ['exceptational_hoilydays'] )
        self.login = os.environ['login'] 
        self.first_order = os.environ['first_order']
        self.second_order = os.environ['second_order']
        self.third_order = os.environ['third_order']
        self.fourth_order = os.environ['fourth_order']
        self.fifth_order = os.environ['fifth_order']
        self.exit_orders = os.environ['exit_orders']
        self.logout_session = os.environ['logout_session']
        return True
    
class Fields : 
    kotak_neo = 'kotak_neo'
    broker_name ='broker_name'
    broker_session = 'broker_session'
    nOrdNo = 'nOrdNo'
    order_id = 'order_id'
    ticker = 'ticker'
    entry_orderid = 'entry_orderid'
    entry_price_initial = 'entry_price_initial'
    sl_orderid = 'sl_orderid'
    sl_price = 'sl_price'
    sl_price_initial = 'sl_price_initial'
    sl_percent = 'sl_percent'
    qty = 'qty'
    transaction_type = 'transaction_type'
    data = 'data'
    token = 'token'
    
    tag = 'tag'
    entry_tag = 'entry_tag'
    sl_tag = 'sl_tag' 
    
    loop_no = 'loop_no'
    stCode = 'stCode'
    price = 'price'
    Buy = 'Buy'
    Sell = 'Sell'
    CE = 'CE'
    PE = 'PE'
    open = "open"
    pending_order = 'pending_order'
    re_entry_open = 're_entry_open'
    reentry_pending_order = 'reentry_pending_order'
    closed = 'closed'
    placed_sucessfully = 'placed_sucessfully'
    sl_hit = 'sl_hit'
     
    option_type = 'option_type'
    entry_price = 'entry_price'
    entry_time = 'entry_time'
    sl_orderid_status = 'sl_orderid_status'
    entry_orderid_status = 'entry_orderid_status'
    exit_reason = 'exit_reason'
    exit_price = 'exit_price'
    exit_time = 'exit_time'
    
    stratagy = 'stratagy'
    NineTwenty = 'NineTwenty'
    NineThirty =  'NineThirty'
    NineFourtyFive = 'NineFourtyFive'
    TenThirty = 'TenThirty'
    Eleven = 'Eleven'
    
    recording = 'recording'
    charges = 'charges'
    drift_points = 'drift_points'
    drift_rs = 'drift_rs'
