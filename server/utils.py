import pymongo
import os
from  datetime import datetime as dt,timedelta,time as time_
from dotenv import load_dotenv
from typing import Dict, List 
import requests
from bs4 import BeautifulSoup
import ast
import datetime
import time
import logging


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

def setup_daily_logger(empty:bool = False):
    log_directory = 'log'
    today = str(dt.today().date())
    log_filename = os.path.join(log_directory, f'{today}.log')
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()  # Also print to console
        ]
    )
    # logging.info('Logger setup complete')
    return logging

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
                
                # send_message(message = f'hoilyday_dict : {hoilyday_dict}')
                if (date in hoilyday_dict) or (weekday in hoilyday_dict):
                    reason = hoilyday_dict[date] if date in hoilyday_dict else hoilyday_dict[weekday]
                    return True, reason
                else:
                    return False, None

            else:
                send_message(message = "Holiday table not found on the page.", emergency = True)
                return False, False
    except Exception as e :     
        send_message(message = "Failed to retrieve NSE holiday data. Status code: {e}", emergency = True)
        return False, False
        
def is_market_time():
    current_time = dt.today().time()
    start_time = time_(hour = 9, minute = 10, second = 0)
    end_time = time_(hour = 15, minute = 30, second = 0)
    if (current_time > start_time) and (current_time < end_time):
        return True
    else : 
        return False
 
def sleep_till_next_day():
    now= dt.now()
    tomorow_9am = (now + timedelta(days=1)).replace(hour = 9, minute = 16, second = 0)
    # tomorow_9am = (now + timedelta(days=0)).replace(hour=9,minute=43,second=0)
    total_seconds = (tomorow_9am-now).total_seconds()
    time.sleep(total_seconds)
       
def get_ist_now():
    return dt.now() + timedelta(0)

def wait_until_next_minute():
    now = dt.now()
    next_minute = (now + timedelta(minutes = 1)).replace(second = 0, microsecond = 0)
    sleep_time = (next_minute - now).total_seconds()
    return sleep_time

def get_available_margin(broker_session,broker_name):
    if broker_name == 'kotak_neo':
        return float(broker_session.limits()['Net'])

def trailing_points():
    points = 0
    if env_variables.index == 'BANKNIFTY' : 
        points = 5
            
    elif env_variables.index == 'NIFTY' : 
        points = 2
        
    elif env_variables.index == 'FINNIFTY' : 
        points = 2
        
    elif env_variables.index == 'MIDCPNIFTY' : 
        points = 1.5
        
    return points
            
def database(day_tracker = False, recording = False):
    mongo_db = pymongo.MongoClient(env_variables.mongodb_link)
    if day_tracker :
        data = mongo_db[f'Performance_{dt.now().year}']
    elif recording : 
        data = mongo_db[Fields.recording]
    else : 
         data = mongo_db[env_variables.database_name]
    return data
        
def set_coloumn_name(df,broker_name):
    if  broker_name == 'kotak_neo' :
        column_name_dict = {
            'nOrdNo':'order_id',
            'ordDtTm':'order_time',
            'trdSym':'order_symbol',
            'tok':'symbol_token',
            'qty':'qty',
            'fldQty':'filled_qty',
            'avgPrc':'price',
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
            'rejRsn' : 'message',
            
            
        }
    if   broker_name =='fyers':
        pass

        
    df.rename(columns = column_name_dict,inplace = True)
    df.to_csv('order.csv')
    return df

def get_qty_option_price(broker_name):
    fifty_per_risk = env_variables.fifty_per_risk
    re_entry_risk = env_variables.re_entry_risk
    wait_trade_risk = env_variables.wait_trade_risk 
    lot_size = env_variables.lot_size
    hedge_cost = env_variables.hedge_cost
    selling_lots = env_variables.selling_lots
    hedge_lots = env_variables.hedge_lots
    

    if  broker_name == 'kotak_neo' : 
        if env_variables.index == 'BANKNIFTY' : 
            fifty_per_qty = lot_size * selling_lots
            fifty_per_price = (fifty_per_risk / lot_size) + hedge_cost
            re_entry_qty = lot_size * selling_lots
            re_entry_price = (re_entry_risk / lot_size) + hedge_cost
            wait_trade_qty  = lot_size * selling_lots  
            wait_trade_price = (wait_trade_risk / lot_size) + hedge_cost
            hedge_qty =  lot_size * hedge_lots 
            
        elif env_variables.index == 'NIFTY' : 
            fifty_per_qty = lot_size * selling_lots
            fifty_per_price = (fifty_per_risk / lot_size) + hedge_cost
            re_entry_qty = lot_size * selling_lots
            re_entry_price = (re_entry_risk / lot_size) + hedge_cost
            wait_trade_qty  = lot_size * selling_lots  
            wait_trade_price = (wait_trade_risk / lot_size) + hedge_cost
            hedge_qty =  lot_size * hedge_lots 
            
        elif env_variables.index == 'FINNIFTY' : 
            fifty_per_qty = lot_size * selling_lots
            fifty_per_price = (fifty_per_risk / lot_size) + hedge_cost
            re_entry_qty = lot_size * selling_lots
            re_entry_price = (re_entry_risk / lot_size) + hedge_cost
            wait_trade_qty  = lot_size * selling_lots  
            wait_trade_price = (wait_trade_risk / lot_size) + hedge_cost
            hedge_qty =  lot_size * hedge_lots 
        
        elif env_variables.index == 'MIDCPNIFTY' : 
            fifty_per_qty = lot_size * (round(selling_lots))
            fifty_per_price = (fifty_per_risk / lot_size) + hedge_cost
            re_entry_qty = lot_size * (round(selling_lots))
            re_entry_price = (re_entry_risk / lot_size) + hedge_cost
            wait_trade_qty  = lot_size * (round(selling_lots))  
            wait_trade_price = (wait_trade_risk / lot_size) + hedge_cost
            hedge_qty =  lot_size * (round(hedge_lots)) 
            
    return fifty_per_qty, fifty_per_price, re_entry_qty, re_entry_price, wait_trade_qty, wait_trade_price, hedge_qty
            
class env_variables:
    env_variable_initilised = False
    option_chain_set = False
    today = None
    thread_list = []
    socket_open = False
    logger = None #setup_daily_logger(True)
    lot_size : int
    selling_lots : int
    hedge_lots : int
    hedge_cost : float
    index : str 
    expiry_base_instrument : bool
    product_type : str
    
    fifty_per_risk : float
    re_entry_risk : float
    wait_trade_risk  : float
    
    mongodb_link : str
    day_tracker : bool
    database_name : str
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
    FS_First : str
    RE_First : str
    WNT_First : str
    RE_Second : str
    RE_Third : str
    exit_orders : str
    logout_session : str
    Buy_Hedges : str 
    
    capital : str
    qty_partation_loop : int
    telegram_api_dict : dict
    
    @classmethod
    def load_env_variable (self):
        import os
        load_dotenv() 
        
        self.env_variable_initilised= True
        self.option_chain_set = False
        self.thread_list = []
        self.index = None
        self.today = dt.today().date()
        self.lot_size = 1
        self.selling_lots = int(os.environ['selling_lots']) 
        self.hedge_lots = int(os.environ['hedge_lots']) 
        self.hedge_cost = 0
        
        self.expiry_base_instrument = bool(os.environ['expiry_base_instrument'])
        self.day_tracker = bool(os.environ['day_tracker'])
        
        self.mongodb_link = os.environ['mongodb_link'] 
        self.database_name = os.environ['database_name'] 
        
        self.consumer_key = os.environ['consumer_key'] 
        self.secretKey = os.environ['secretKey'] 
        self.mobileNumber = os.environ['mobileNumber'] 
        self.login_password = os.environ['login_password'] 
        self.broker_name = os.environ['broker_name']    
        self.session_validation_key = os.environ['session_validation_key'] 
        self.two_factor_code = os.environ['two_factor_code'] 
        # if self.broker_name == Fields.kotak_neo : 
        self.product_type = 'MIS'
            
        self.allowed_loss_percent = float(os.environ['allowed_loss_percent'])
        # self.hoilydays =os.environ['hoilydays'] 
        self.exceptational_tradingdays =  ast.literal_eval(os.environ['exceptational_tradingdays'] )
        self.exceptational_hoilydays = ast.literal_eval(os.environ['exceptational_hoilydays'] )
        
        self.fifty_per_risk = float(os.environ['fifty_per_risk'])
        self.re_entry_risk = float(os.environ['re_entry_risk'])
        self.wait_trade_risk = float(os.environ['wait_trade_risk'])
        
        # self.login = dt.strftime(get_ist_now(),'%H:%M')
        # self.exit_orders = dt.strftime(get_ist_now(),'%H:%M')
        
        self.login = os.environ['login']
        self.FS_First = os.environ['FS_First']
        self.RE_First = os.environ['RE_First']
        self.Buy_Hedges = os.environ['Buy_Hedges']
        self.WNT_First = os.environ['WNT_First']
        self.RE_Second = os.environ['RE_Second']
        self.RE_Third = os.environ['RE_Third']
        self.exit_orders = os.environ['exit_orders']
        self.logout_session = os.environ['logout_session']  
        self.capital = float(os.environ['capital'])
        self.qty_partation_loop = int(os.environ['qty_partation_loop'])
        self.logger = setup_daily_logger()
        
        self.telegram_api_dict = {
                                Fields.FS_First : os.environ['nine_twenty_bot_token'],
                                Fields.RE_First  : os.environ['nine_thirty_bot_token'],
                                Fields.WNT_First : os.environ['nine_fourty_five_bot_token'],
                                Fields.RE_Second : os.environ['ten_thirty_bot_token'],
                                Fields.RE_Third : os.environ['eleven_bot_token'],
                                Fields.Hedges : os.environ['nine_twenty_bot_token'],   #passing hedges updates to Nine Twenty
                                
                                'common_logger' : os.environ['common_loggger'],
                                'emergency' : os.environ['emergency_bot_token'],
                                'chat_id' : os.environ['chatId']
                                }
            
        return True
    
class Fields : 
    
    # Entry Fields
    entry_orderid = 'entry_orderid'
    entry_price_initial = 'entry_price_initial'
    entry_order_count = 'entry_order_count'
    entry_order_execuation_type = 'entry_order_execuation_type'
    entry_price = 'entry_price'
    entry_time = 'entry_time'
    entry_orderid_status = 'entry_orderid_status'
    entry_tag = 'entry_tag'
    
    # Exit Field
    exit_orderid = 'exit_orderid'
    exit_price_initial = 'exit_price_initial'
    exit_order_count = 'exit_order_count'
    exit_order_execuation_type = 'exit_order_execuation_type'
    exit_reason = 'exit_reason'
    exit_price = 'exit_price'
    exit_time = 'exit_time'
    exit_orderid_status = 'exit_orderid_status'
    exit_tag = 'exit_tag' 
    exit_price = 'exit_price'
    exit_percent = 'exit_percent'
    
    # Common firlds in orders
    qty = 'qty'
    Buy = 'Buy'
    Sell = 'Sell'
    product_type = 'product_type'
    price = 'price'
    loop_no = 'loop_no'
    recording = 'recording'
    index = 'index'
    charges = 'charges'
    pl = 'pl'
    free_margin = 'free_margin'
    drift_points = 'drift_points'
    drift_rs = 'drift_rs'
    transaction_type = 'transaction_type'
    stCode = 'stCode'
    CE = 'CE'
    PE = 'PE'
    
    # Order Status with reason
    limit_order = 'limit_order'
    market_order = 'market_order'
    
    open = "open"
    re_entry_open = 're_entry_open'
    # pending_order = 'pending_order'
    # reentry_pending_order = 'reentry_pending_order'
    closed = 'closed'
    rejected = 'rejected'
    # placed_sucessfully = 'placed_sucessfully'
    
    sl_hit = 'sl_hit'
    day_end = 'day_end'
    
    # Staratgy Names
    stratagy = 'stratagy'
    FS_First = 'FS_First'
    RE_First =  'RE_First'
    WNT_First = 'WNT_First'
    RE_Second = 'RE_Second'
    RE_Third = 'RE_Third'
    Hedges = 'Hedges'
    
    # Other
    kotak_neo = 'kotak_neo'
    broker_name ='broker_name'
    broker_session = 'broker_session'
    nOrdNo = 'nOrdNo'
    order_id = 'order_id'
    ticker = 'ticker'
    data = 'data'
    token = 'token'
    tag = 'tag'
    option_type = 'option_type'
    
def send_message(message,stratagy = None, emergency = False, send_image = False):
    telegram_api_dict = env_variables.telegram_api_dict
    current_time = get_ist_now()
    bot_chatId = telegram_api_dict['chat_id' ]
    if not send_image : 
        if emergency : 
            bot_token = telegram_api_dict['emergency']
            env_variables.logger.warning(f'{message}\n')
            
        elif stratagy == None: 
            bot_token = telegram_api_dict['common_logger']
            env_variables.logger.info(f'{message}\n')

        else : 
            bot_token = telegram_api_dict[stratagy]
            env_variables.logger.info(f'{message}\n')
            
        message = f'{dt.strftime(current_time,"%H:%M:%S")}\n{message}'
        response = requests.post(f'https://api.telegram.org/bot{bot_token}/sendMessage', json = {'chat_id': bot_chatId,'text': message})
        if response.status_code != 200:
            message = f"{stratagy}_bot not able to send message Reason : {response.text}" 
            env_variables.logger.warning(f'Retry Trigger_finder responce : {message}')
            bot_token = telegram_api_dict['emergency']
            message = f'{dt.strftime(current_time,"%H:%M:%S")}\n{message}'
            response = requests.post(f'https://api.telegram.org/bot{bot_token}/sendMessage', json = {'chat_id': bot_chatId,'text': message})
            env_variables.logger.warning(f'Responce from retry : {response}')
            
            
    if send_image : 
        with open(message, 'rb') as file:
            bot_token = telegram_api_dict['common_logger']
            url = url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
            response = requests.post(url, data={'chat_id': bot_chatId}, files={'photo': file})
            if response.status_code != 200:
                message = ("Not able to send image")
                bot_token = telegram_api_dict['emergency']
                env_variables.logger.warning(f'Retry Trigger_finder responce : {message}') 
                response = requests.post(f'https://api.telegram.org/bot{bot_token}/sendMessage', json = {'chat_id': bot_chatId,'text': message})
                env_variables.logger.warning(f'Responce from retry-image : {response}')