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
    today = dt.now().strftime('%Y-%m-%d')
    log_filename = os.path.join(log_directory, f'{today}.log')
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()  # Also print to console
        ]
    )
    logging.info('Logger setup complete')
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
    start_time = time_(hour=9,minute=15,second=30)
    end_time = time_(hour=15,minute=30,second=0)
    if (current_time > start_time) and (current_time < end_time):
        return True
    else : 
        return False
 
def sleep_till_next_day():
    now= dt.now()
    tomorow_9am = (now + timedelta(days=1)).replace(hour=9,minute=16,second=15)
    # tomorow_9am = (now + timedelta(days=0)).replace(hour=9,minute=43,second=0)
    total_seconds = (tomorow_9am-now).total_seconds()
    time.sleep(total_seconds)
       
def get_ist_now():
    return dt.now() + timedelta(0)

def trailing_points():
    if env_variables.index == 'BANKNIFTY' : 
        points = 5
            
    elif env_variables.index == 'NIFTY' : 
        points = 3
        
    elif env_variables.index == 'FINNIFTY' : 
        points = 3
        
    elif env_variables.index == 'MIDCPNIFTY' : 
        points = 3
        
    return points
            
def get_db():
    mongo_db = pymongo.MongoClient(env_variables.mongodb_link)
    data = mongo_db[env_variables.database_name]
    data = data[str(dt.today().date())]
    # data = data['2024-06-20']
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
    fifty_per_risk = 2250
    re_entry_risk = 1875
    wait_trade_risk  = 1500
    lot_size = env_variables.lot_size
    

    if  broker_name == 'kotak_neo' : 
        if env_variables.index == 'BANKNIFTY' : 
            fifty_per_qty = lot_size
            fifty_per_price = fifty_per_risk / lot_size
            re_entry_qty = lot_size
            re_entry_price = re_entry_risk / lot_size
            wait_trade_qty  = lot_size  
            wait_trade_price = wait_trade_risk / lot_size
            
        elif env_variables.index == 'NIFTY' : 
            fifty_per_qty = lot_size
            fifty_per_price = fifty_per_risk / lot_size
            re_entry_qty = lot_size
            re_entry_price = re_entry_risk / lot_size
            wait_trade_qty  = lot_size  
            wait_trade_price = wait_trade_risk / lot_size
            
        elif env_variables.index == 'FINNIFTY' : 
            fifty_per_qty = lot_size
            fifty_per_price = fifty_per_risk / lot_size
            re_entry_qty = lot_size
            re_entry_price = re_entry_risk / lot_size
            wait_trade_qty  = lot_size  
            wait_trade_price = wait_trade_risk / lot_size
        
        elif env_variables.index == 'MIDCPNIFTY' : 
            fifty_per_qty = lot_size
            fifty_per_price = fifty_per_risk / lot_size
            re_entry_qty = lot_size
            re_entry_price = re_entry_risk / lot_size
            wait_trade_qty  = lot_size  
            wait_trade_price = wait_trade_risk / lot_size
            
    return fifty_per_qty, fifty_per_price, re_entry_qty, re_entry_price, wait_trade_qty, wait_trade_price
            
class env_variables:
    env_variable_initilised = False
    option_chain_set = False
    today = None
    thread_list = []
    socket_open = False
    logger = None #setup_daily_logger(True)
    lot_size : int
    index : str 
    expiry_base_instrument : bool
    product_type : str
    
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
    NineTwenty : str
    NineThirty : str
    NineFourtyFive : str
    TenThirty : str
    Eleven : str
    exit_orders : str
    logout_session : str
    
    capital : str
    qty_partation_loop : int
    
    @classmethod
    def load_env_variable (self):
        import os
        load_dotenv() 
        
        self.env_variable_initilised= True
        self.option_chain_set = False
        self.thread_list = []
        self.today = dt.today().date()
        self.lot_size = 1
        self.index = ''
        
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
        if self.broker_name == Fields.kotak_neo : 
            self.product_type = 'MIS'
            
        self.allowed_loss_percent = float(os.environ['allowed_loss_percent'] )
        # self.hoilydays =os.environ['hoilydays'] 
        self.exceptational_tradingdays =  ast.literal_eval(os.environ['exceptational_tradingdays'] )
        self.exceptational_hoilydays = ast.literal_eval(os.environ['exceptational_hoilydays'] )
        
        # self.login = dt.strftime(get_ist_now(),'%H:%M')
        # self.exit_orders = dt.strftime(get_ist_now(),'%H:%M')
        
        self.login = os.environ['login']
        self.NineTwenty = os.environ['first_order']
        self.NineThirty = os.environ['second_order']
        self.NineFourtyFive = os.environ['third_order']
        self.TenThirty = os.environ['fourth_order']
        self.Eleven = os.environ['fifth_order']
        self.exit_orders = os.environ['exit_orders']
        self.logout_session = os.environ['logout_session']  
        self.capital = float(os.environ['capital'])
        self.qty_partation_loop = int(os.environ['qty_partation_loop'])
        self.logger = setup_daily_logger()
        logger_bot('env variable initilised')    
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
    NineTwenty = 'NineTwenty'
    NineThirty =  'NineThirty'
    NineFourtyFive = 'NineFourtyFive'
    TenThirty = 'TenThirty'
    Eleven = 'Eleven'
    
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
    
  
def emergency_bot(bot_message):
    """ It is used for sending alert to Emergeny situation"""
    current_time = get_ist_now()
    env_variables.logger.warning(f'{bot_message}\n')
    bot_message = f'{dt.strftime(current_time,"%H:%M:%S")}\n{bot_message}'
    bot_token = os.environ['emergency_bot_token']
    bot_chatId = os.environ['chatId']
    # print(bot_message,'\n')
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatId + '&parse_mode=Markdown&text=' + bot_message
    response = requests.get(send_text)
    if response.status_code != 200:
        alert_bot("emergency_bot not able to send message")
        
def alert_bot(bot_message_ : str,send_image : bool = False):
    """ It is used for sending price alert"""
    current_time = get_ist_now()
    bot_message = f'{dt.strftime(current_time,"%H:%M:%S")}\n{bot_message_}'
    bot_token = os.environ['alert_bot_token']
    bot_chatId = os.environ['chatId']
    
    if send_image : 
        with open('plot.png', 'rb') as file:
            url = url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
            response = requests.post(url, data={'chat_id': bot_chatId}, files={'photo': file})
            if response.status_code != 200:
                emergency_bot("alert_bot not able to send image")
    else : 
        # print(bot_message,'\n')
        env_variables.logger.info(f'{bot_message}\n')

        send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatId + '&parse_mode=Markdown&text=' + bot_message
        response = requests.get(send_text)

def logger_bot(bot_message):
    """ It is used for sending order manangemant"""
    current_time = get_ist_now()
    env_variables.logger.info(f'{bot_message}\n')
    bot_message = f'{dt.strftime(current_time,"%H:%M:%S")} {bot_message}'
    bot_token = os.environ['logger_bot_token']
    bot_chatId = os.environ['chatId']
    # print(bot_message,'\n')
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatId + '&parse_mode=Markdown&text=' + bot_message
    response = requests.get(send_text)
    if response.status_code != 200:
        emergency_bot("logger_bot not able to send message")

def Trigger_finder(bot_message):
    """ It is used for sending alert to Emergeny situation"""
    current_time = get_ist_now()
    env_variables.logger.info(f'{bot_message}\n')
    bot_message = f'{dt.strftime(current_time,"%H:%M:%S")}\n{bot_message}'
    bot_token = os.environ['Trigger_finder_token']
    bot_chatId = os.environ['chatId']
    # print(bot_message,'\n')
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatId + '&parse_mode=Markdown&text=' + bot_message
    response = requests.get(send_text)
    if response.status_code != 200:
        emergency_bot("Trigger_finder not able to send message")
    
