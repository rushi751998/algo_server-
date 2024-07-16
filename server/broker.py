import neo_api_client
from server.engine import login_Engine
from server.utils import send_message
import pandas as pd
from  datetime import datetime as dt
from dateutil.relativedelta import relativedelta
import time
import pandas as pd
import threading
from server.utils import set_coloumn_name, order_staus_dict, env_variables as env, Fields as F
nan = 'nan'

option_chain = {}
ticker_to_token= {}


class Login(login_Engine):
    
    
    def __init__(self):
        pass

    def setup(self):
        
        try : 
            self.broker_name = env.broker_name
            session_validation_key,broker_session = self.login()
        except Exception as e :
            send_message(message = f'Facing Issue in def login \nissue :{e}', emergency = True)
        
        check_validation_key = env.session_validation_key
        if session_validation_key == check_validation_key:
            return True,broker_session

        else:
            send_message(message = 'Not able to Login issue in session_validation_key', emergency = True)
            return False, None
            

    def login(self):
        if self.broker_name == F.kotak_neo :
            broker_session = neo_api_client.NeoAPI(consumer_key=env.consumer_key,
                               consumer_secret=env.secretKey, environment='prod',
                               access_token=None, neo_fin_key=None)
            broker_session.login(mobilenumber=env.mobileNumber, password=env.login_password)
            session = broker_session.session_2fa(env.two_factor_code)
            return session[F.data]['greetingName'],broker_session 
            
        else : 
            emergency_bot ("Not ale to get broker_name ", emergency = True)
            
    
class Order_details : 
    
    def __init__(self,broker_session,broker_name):
        self.broker_session = broker_session
        self.broker_name = broker_name

    def order_book(self):
        if self.broker_name == F.kotak_neo :
            all_orders,filled_order,pending_order = pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
            responce = self.broker_session.order_report()
            # print(responce[F.data][0])
            try :
                if responce[F.stCode] == 200 : 
                    all_orders = pd.DataFrame(responce[F.data])[[F.nOrdNo,'ordDtTm','trdSym','tok',F.qty,'fldQty','avgPrc','trnsTp','prod' ,'exSeg','ordSt','stkPrc','optTp','brdLtQty','expDt','GuiOrdId','rejRsn']]
                    all_orders = set_coloumn_name(all_orders,self.broker_name)
                    all_orders = all_orders[all_orders['exchange_segement']=='nse_fo']
                    filled_order = all_orders[all_orders['order_status']=='complete']
                    # pending_order = all_orders[all_orders['order_status'] == 'open']
                    pending_order = all_orders[all_orders['order_status'].isin(['trigger pending','open'])]
                    return True,all_orders,filled_order,pending_order
            except KeyError:
                env.logger.warning(f"Func : Order_details.order_book Input:  Output: {False} Error : KeyError \n{responce}")
                return False,all_orders,filled_order,pending_order

            except Exception as e:
                send_message(message = f'Not able to get orderbook\nMessage : {e}\nresponce : {responce}', emergency = True)
                return False,all_orders,filled_order,pending_order
                
    def position_book(self):
        if self.broker_name == F.kotak_neo :
            responce = self.broker_session.positions()
            try :
                if responce[F.stCode] == 200 : 
                    responce_code = None if responce[F.stCode] == 200 else send_message(message = f"Not able to get position_book due to : {order_staus_dict[responce[F.stCode]]}", emergency = True)
                    all_positions = pd.DataFrame(responce[F.data]) [['trdSym','type','optTp','buyAmt' ,'prod','exSeg','tok','flBuyQty','flSellQty','sellAmt','stkPrc','expDt',]]
                    all_positions = set_coloumn_name(all_positions, self.broker_name)
                    option_positions = all_positions[all_positions[F.option_type].isin([F.CE, F.PE])]
                    open_position = option_positions[option_positions['filed_buy_qty'] != option_positions['filed_sell_qty']]
                    closed_position = option_positions[option_positions['filed_buy_qty'] == option_positions['filed_sell_qty']]
                    return all_positions,open_position,closed_position
            except KeyError:
                env.logger.warning(f"Func : Order_details.position_book Input:  Output: {False} Error : KeyError\n{responce}")
                return None,None,None

            except Exception as e:
                send_message(message = f'Not able to get position_book\nMessage : {e}\nresponce : {responce}')
                return None,None,None

class Socket_handling:
    future_token : str
    
    def __init__(self,broker_name,broker_session):
        self._lock = threading.Lock()  # Lock for synchronizing access
        self.broker_name= broker_name
        self.broker_session = broker_session
        self.is_prepared = False
    
    def start_socket(self,expiry_base_instrument):
        if not self.is_prepared : 
            try : 
                df,future_token,is_prepared = self.prepare_option_chain_Future_token(expiry_base_instrument)
                with self._lock:
                            self.future_token = future_token
                            self.is_prepared = is_prepared
            except Exception as e :
                send_message(message = f'Facing Issue in prepare_option_chain_Future_token \nissue : {e}', emergency = True)
                    
        if self.broker_name == F.kotak_neo : 
            if self.is_prepared : 
                # print(self.is_prepared)
                token_list = [{"instrument_token":i,"exchange_segment":'nse_fo'} for i in option_chain.keys()]
                try : 
                    self.broker_session.on_message = self.update_option_chain  # called when message is received from websocket
                    self.broker_session.on_error = self.on_error  # called when any error or exception occurs in code or websocket
                    self.broker_session.on_open = self.on_open  # called when websocket successfully connects
                    self.broker_session.on_close = self.on_close  # called when websocket connection is closed
                    self.broker_session.subscribe(token_list, isIndex=False, isDepth=False)
                    self.stocket_started = True
                except Exception as e:
                    send_message(message = f'Facing Issue in Socket.start \nissue : {e}', emergency = True)
        
    def on_error(self,message):
        env.socket_open = False
        send_message(message = f'Issue in Socket : {message}', emergency = True)
                
    def on_open(self,message):
        env.socket_open = True
        env.option_chain_set = True
        send_message(message = f'Socket Started : {message}')
        
    def on_close(self,message):
        env.socket_open = False
        send_message(message = f'Socket Stopped : {message}')
            
    def update_option_chain(self, message):
        for tick in message[F.data]:
            try:
                token = tick['tk']
                volume =int( tick['v'])
                with self._lock:
                    option_chain[token]['v'] = volume
            except:
                pass

            try:
                token = tick['tk']
                oi = int(tick['oi'])
                with self._lock:
                    option_chain[token]['oi'] = oi
            except:
                pass

            try:
                token = tick['tk']
                ltp = float(tick['ltp'])
                with self._lock:
                    option_chain[token]['ltp'] = ltp
            except:
                pass
        # print(option_chain,'\n\n')

    def prepare_option_chain_Future_token(self,expiry_base_instrument): 
        if self.broker_name == F.kotak_neo : 
            script_master =   [i for i in  self.broker_session.scrip_master()['filesPaths'] if 'nse_fo' in i]
            df = pd.read_csv(script_master[0],low_memory=False)
            df.columns=[i.strip() for i in df.columns]
            # df = df[df['pSymbolName'].isin(['NIFTY', 'FINNIFTY','BANKNIFTY' , 'MIDCPNIFTY'])][['pSymbol','pSymbolName','pTrdSymbol','pOptionType','pScripRefKey','lLotSize','lExpiryDate','dStrikePrice;', 'iMaxOrderSize', 'iLotSize', 'dOpenInterest']]
            df = df[df['pSymbolName'].isin(['NIFTY','BANKNIFTY'])][['pSymbol','pSymbolName','pTrdSymbol','pOptionType','pScripRefKey','lLotSize','lExpiryDate','dStrikePrice;', 'iMaxOrderSize', 'iLotSize', 'dOpenInterest']]
            df['lExpiryDate'] = df['lExpiryDate'].apply(lambda x:dt.fromtimestamp(x).date()+ relativedelta(years=10))
            df = df[['pSymbol','pScripRefKey','pSymbolName','lExpiryDate','dStrikePrice;','pOptionType','lLotSize']]
            df.columns = ['token','ticker','index','expiry','strike','optionType','lotSize']
            df.dropna(inplace=True)
            df['days_to_expire'] = df['expiry'].apply(lambda x:(dt.strptime(str(x),'%Y-%m-%d').date()-dt.today().date()).days)
            df.sort_values('days_to_expire',inplace=True)

            option_tickers = df[df['optionType'] != 'XX']
            future_tickers = df[df['optionType'] == 'XX']

            if expiry_base_instrument:
                future_tickers = future_tickers[(future_tickers['days_to_expire'] == future_tickers['days_to_expire'].min())]
                future_token = future_tickers[future_tickers['days_to_expire'] == future_tickers['days_to_expire'].min()].iloc[0]['token']
                
                option_tickers = option_tickers[(option_tickers['days_to_expire'] == option_tickers['days_to_expire'].min())]
                index_list = option_tickers['index'].unique()
                
                if len(index_list) >= 1:
                    if 'BANKNIFTY' in index_list : 
                        index = 'BANKNIFTY'
                    elif 'NIFTY' in index_list : 
                        index = 'NIFTY'
                    elif 'FINNIFTY' in index_list : 
                        index = 'FINNIFTY'
                    elif 'MIDCPNIFTY' in index_list : 
                        index = 'MIDCPNIFTY'
                    # print(index)
                    option_tickers = option_tickers[option_tickers['index'] == index]
                    future_tickers = future_tickers[future_tickers['index'] == index]
                
            else:
                index = 'BANKNIFTY'
                option_tickers = option_tickers[option_tickers['index'] == index]
                future_tickers = future_tickers[future_tickers['index'] == index]
                
                future_tickers = future_tickers[(future_tickers['days_to_expire'] == future_tickers['days_to_expire'].min())]
                future_token = future_tickers[future_tickers['days_to_expire'] == future_tickers['days_to_expire'].min()].iloc[0]['token']
                
                option_tickers = option_tickers[(option_tickers['days_to_expire'] == option_tickers['days_to_expire'].min())]
            env.index = index
            # print(index)
            df = pd.concat([option_tickers, future_tickers])
            df.reset_index(inplace=True,drop=True)

            with self._lock:
                env.index = index
                env.lot_size = int(df.iloc[0]['lotSize'])
                
                for index,row in df.iterrows():
                    option_chain[row['token']] = {'v':0,'oi':0,'ltp':0,'option_type':row['optionType'],'ticker':row['ticker']}
                    ticker_to_token[row['ticker']]=row['token']
            send_message(message = f'prepared opetion chain\nTodays instrument : {env.index}')
            return df, future_token,True
        
def get_option_chain():
    return option_chain

def get_symbol(option_type,broker_name,option_price = None, is_hedge = False):
    if broker_name == F.kotak_neo :
        chain = pd.DataFrame(get_option_chain()).T
        chain = chain[(chain['v'] > 100000) & (chain['oi'] > 100000)]
        chain = chain[chain[F.option_type]==option_type]
        
        if not is_hedge :
            strike = chain[chain['ltp']<=option_price].sort_values('ltp',ascending=False).iloc[0]
            env.logger.info(f"Func : get_symbol Input: {option_type},{option_price},{broker_name} Output: {strike['ticker']}, {strike['ltp']}")
        
        else : 
            strike = chain[chain['ltp'] >= 1].sort_values('ltp',ascending=True).iloc[0]
            env.logger.info(f"Func : get_symbol Input:  Output: {strike['ticker']}, {strike['ltp']}")
            env.hedge_cost = 0
        return strike['ticker'], strike['ltp']

def get_ltp(instrument_token,broker_name):
    if broker_name == F.kotak_neo :
        try:
            ltp = option_chain[instrument_token]['ltp']
            # env.logger.info(f"Token: {instrument_token} LTP: {ltp}")
            return ltp
        except Exception as e : 
            send_message(message = f"not able to get lpt", emergency = True)
        
def set_hedge_cost(broker_name):
    """Setup hedge cost at max from both CE and PE"""
    if broker_name == F.kotak_neo :
        if env.hedge_cost == 0 : 
            hedge_cost = 0
            for i in [F.CE,F.PE]:
                chain = pd.DataFrame(get_option_chain()).T
                chain = chain[chain[F.option_type] == i]
                chain = chain[(chain['v'] > 100000) & (chain['oi'] > 100000)]
                chain = chain[chain['ltp'] >= 1].sort_values('ltp')
                cost = chain.iloc[0]['ltp']
                if cost > hedge_cost :
                    hedge_cost = cost
            env.hedge_cost = hedge_cost
            send_message(message = f"Func : set_hedge_cost Output: {hedge_cost}")
            return True  
    
def get_token(ticker):
    return ticker_to_token[ticker]

def is_order_rejected_func(order_id,broker_session,broker_name):
    time.sleep(5)
    order_details = Order_details(broker_session, broker_name)
    is_not_empty, all_orders,filled_order, pending_order = order_details.order_book()
    order_status = all_orders[all_orders['order_id'] == str(order_id)].iloc[0]
    
    if order_id != 0 : 
        if broker_name == 'kotak_neo' :
            if order_status['order_status'] == 'rejected':
                if ('MIS PRODUCT TYPE BLOCKED' in order_status["message"]) or ('MIS TRADING NOT ALLOWED' in order_status["message"]) : 
                    send_message(message = f'Order rejected\nOrder ID : {order_id}\nProduct Type : {env.product_type}\nMessage : {order_status["message"]}\nPlacing NRML order..', emergency = True)
                    return True, True
                # add more rejection types here
                else : 
                    send_message(message = f'Order rejected\nOrder ID : {order_id}\nProduct Type : {env.product_type}\nMessage : {order_status["message"]}', emergency = True)
                    return True, False
                    
            else : 
                return False, False
        # Add other broker code here
            
    else : 
        return True , False
