
from server.broker import Login, get_symbol, Order_details, Socket_handling, option_chain, ticker_to_token, set_hedge_cost
from server.checking import Checking
from server.utils import (get_ist_now,
                   is_hoilyday,
                   is_market_time,
                   sleep_till_next_day,
                   env_variables as env ,
                   database ,F,
                   get_qty_option_price,
                   wait_until_next_minute,
                   is_straragy_traded
                   )

from server.utils import send_message
from server.order_management import Order_management
from neo_api_client import NeoAPI
import time,re,pymongo,os ,pandas as pd 
from threading import Thread
import threading
from  datetime import datetime as dt
pd.options.mode.chained_assignment = None # it is mandatory

from random import randint
threads_ls = []

def socket_thread_fun(**kwargs):
    Socket_handling(broker_name,broker_session).start_socket()

def order_placer(option_type,price,loop_no,stratagy,exit_percent,qty,transaction_type,broker_name,broker_session,wait_percent = None):

    if stratagy == F.Hedges : 
        ticker, ltp = get_symbol(option_type = option_type, broker_name = broker_name, is_hedge = True)   # Place only hedge orders
        price = ltp
        env.logger.info(f'Placing order  : {ticker}, qty = {qty}, transaction_type = {transaction_type}, stratagy = {stratagy}, exit_percent = {exit_percent}, loop_no = {loop_no} ,price = {price}, option_type = {option_type}')
        Order_management(broker_name,broker_session).order_place(ticker, qty = qty, transaction_type = transaction_type, stratagy = stratagy, exit_percent = exit_percent, loop_no = loop_no ,price = price, option_type = option_type)
    else :
        ticker, ltp = get_symbol(option_type = option_type, option_price = price, broker_name = broker_name)
        price = round(ltp * ((100-wait_percent)/100),1) if wait_percent != None else ltp
        env.logger.info(f'Placing order  : {ticker}, qty = {qty}, transaction_type = {transaction_type}, stratagy = {stratagy}, exit_percent = {exit_percent}, loop_no = {loop_no} ,price = {price}, option_type = {option_type}')
        Order_management(broker_name,broker_session).order_place(ticker, qty = qty, transaction_type = transaction_type, stratagy = stratagy, exit_percent = exit_percent, loop_no = loop_no ,price = price, option_type = option_type)
        
    # except Exception as e : 
    #     send_message(f'Issue in order placer : {e}', emergency = True)
        
def day_end(broker_name,broker_session,option_type):
    try : 
        Order_management(broker_name,broker_session).exit_orders_dayend(option_type)
    except Exception as e : 
        send_message(message = f'Issue in day_end : {e}', emergency = True)

def placing(current_time, broker_name, broker_session):
    is_set_hedge_cost = set_hedge_cost(broker_name)    
    # fifty_per_qty, fifty_per_price, re_entry_qty, re_entry_price, wait_trade_qty, wait_trade_price, hedge_qty = get_qty_option_price(broker_name)
    # print(env.index,re_entry_qty, re_entry_price)
    qty = 25
    hedge_qty = 150
    is_expiry = env.days_to_expiry in [0,1]
    traded_df = pd.DataFrame(database[str(env.today)].find())
    
    if current_time >= env.FS_First and not is_straragy_traded(F.FS_First,traded_df):
        if is_expiry :
            ce_thread = Thread(name = f'CE_{F.Hedges}_{i}_Thread', target = order_placer, kwargs = {F.option_type : F.CE, F.price : 0, F.loop_no : 1, F.stratagy : F.FS_First+"_"+F.Hedges, F.exit_percent : 100, F.qty : qty, F.transaction_type : F.Buy, F.broker_name : broker_name, F.broker_session : broker_session})
            pe_thread = Thread(name = f'PE_{F.Hedges}_{i}_Thread', target = order_placer, kwargs = {F.option_type : F.PE, F.price : 0, F.loop_no : 1, F.stratagy : F.FS_First+"_"+F.Hedges, F.exit_percent : 100, F.qty : qty, F.transaction_type : F.Buy, F.broker_name : broker_name, F.broker_session : broker_session})
            ce_thread.start()
            pe_thread.start()
            ce_thread.join()
            pe_thread.join()
        for i in range(env.qty_partation_loop):
            # i = 2
            ce_thread = Thread(name = f'CE_{F.FS_First}_{i}_Thread', target = order_placer, kwargs = {F.option_type : F.CE, F.price : 40, F.loop_no : i, F.stratagy : F.FS_First, F.exit_percent : 25, F.qty : qty, F.transaction_type : F.Sell, F.broker_name : broker_name, F.broker_session : broker_session, 'wait_percent' : 5})
            pe_thread = Thread(name = f'PE_{F.FS_First}_{i}_Thread', target = order_placer, kwargs = {F.option_type : F.PE, F.price : 40, F.loop_no : i, F.stratagy : F.FS_First, F.exit_percent : 25, F.qty : qty, F.transaction_type : F.Sell, F.broker_name : broker_name, F.broker_session : broker_session, 'wait_percent' : 5})
            ce_thread.start()
            pe_thread.start()
            ce_thread.join()
            pe_thread.join()

    elif current_time >= env.FS_Second and not is_straragy_traded(F.FS_Second,traded_df):
        if is_expiry :
            ce_thread = Thread(name = f'CE_{F.Hedges}_{i}_Thread', target = order_placer, kwargs = {F.option_type : F.CE, F.price : 0, F.loop_no : i, F.stratagy : F.FS_Second+"_"+F.Hedges, F.exit_percent : 100, F.qty : qty, F.transaction_type : F.Buy, F.broker_name : broker_name, F.broker_session : broker_session})
            pe_thread = Thread(name = f'PE_{F.Hedges}_{i}_Thread', target = order_placer, kwargs = {F.option_type : F.PE, F.price : 0, F.loop_no : i, F.stratagy : F.FS_Second+"_"+F.Hedges, F.exit_percent : 100, F.qty : qty, F.transaction_type : F.Buy, F.broker_name : broker_name, F.broker_session : broker_session})
            ce_thread.start()
            pe_thread.start()
            ce_thread.join()
            pe_thread.join()
        
        for i in range(env.qty_partation_loop):
            # i = 2
            ce_thread = Thread(name = f'CE_{F.FS_Second}_{i}_Thread', target = order_placer, kwargs = {F.option_type : F.CE, F.price : 40, F.loop_no : i, F.stratagy : F.FS_Second, F.exit_percent : 25, F.qty : qty, F.transaction_type : F.Sell, F.broker_name : broker_name, F.broker_session : broker_session, 'wait_percent' : 5})
            pe_thread = Thread(name = f'PE_{F.FS_Second}_{i}_Thread', target = order_placer, kwargs = {F.option_type : F.PE, F.price : 40, F.loop_no : i, F.stratagy : F.FS_Second, F.exit_percent : 25, F.qty : qty, F.transaction_type : F.Sell, F.broker_name : broker_name, F.broker_session : broker_session, 'wait_percent' : 5})
            ce_thread.start()
            pe_thread.start()
            ce_thread.join()
            pe_thread.join()
            
    elif current_time >= env.Buy_Hedges and not is_straragy_traded(F.Hedges,traded_df)  and (env.days_to_expiry not in expiry_day):
        for i in range(env.qty_partation_loop):
            # i = 15
            ce_thread = Thread(name = f'CE_{F.Hedges}_{i}_Thread', target = order_placer, kwargs = {F.option_type : F.CE, F.price : 0, F.loop_no : 1, F.stratagy : F.Hedges, F.exit_percent : 100, F.qty : hedge_qty, F.transaction_type : F.Buy, F.broker_name : broker_name, F.broker_session : broker_session})
            pe_thread = Thread(name = f'PE_{F.Hedges}_{i}_Thread', target = order_placer, kwargs = {F.option_type : F.PE, F.price : 0, F.loop_no : 1, F.stratagy : F.Hedges, F.exit_percent : 100, F.qty : hedge_qty, F.transaction_type : F.Buy, F.broker_name : broker_name, F.broker_session : broker_session})
            ce_thread.start()
            pe_thread.start()
            ce_thread.join()
            pe_thread.join()
            
    elif current_time >= env.FS_Third and not is_expiry:
        if is_expiry :
            ce_thread = Thread(name = f'CE_{F.Hedges}_{i}_Thread', target = order_placer, kwargs = {F.option_type : F.CE, F.price : 0, F.loop_no : 1, F.stratagy : F.FS_Third+"_"+F.Hedges, F.exit_percent : 100, F.qty : qty, F.transaction_type : F.Buy, F.broker_name : broker_name, F.broker_session : broker_session})
            pe_thread = Thread(name = f'PE_{F.Hedges}_{i}_Thread', target = order_placer, kwargs = {F.option_type : F.PE, F.price : 0, F.loop_no : 1, F.stratagy : F.FS_Third+"_"+F.Hedges, F.exit_percent : 100, F.qty : qty, F.transaction_type : F.Buy, F.broker_name : broker_name, F.broker_session : broker_session})
            ce_thread.start()
            pe_thread.start()
            ce_thread.join()
            pe_thread.join()
        for i in range(env.qty_partation_loop):
            # i = 2
            ce_thread = Thread(name = f'CE_{F.FS_Third}_{i}_Thread', target = order_placer, kwargs = {F.option_type: F.CE, F.price : 30 , F.loop_no : i, F.stratagy : F.FS_Third, F.exit_percent : 25, F.qty : qty, F.transaction_type : F.Sell, F.broker_name : broker_name, F.broker_session : broker_session, 'wait_percent' : 5})
            pe_thread = Thread(name = f'PE_{F.FS_Third}_{i}_Thread', target = order_placer, kwargs = {F.option_type: F.PE, F.price : 30 , F.loop_no : i, F.stratagy : F.FS_Third, F.exit_percent : 50, F.qty : qty, F.transaction_type : F.Sell, F.broker_name : broker_name, F.broker_session : broker_session, 'wait_percent' : 5})
            ce_thread.start()
            pe_thread.start()
            ce_thread.join()
            pe_thread.join()
            
    elif current_time >= env.FS_Fourth and not is_expiry:
        if is_expiry :
            ce_thread = Thread(name = f'CE_{F.Hedges}_{i}_Thread', target = order_placer, kwargs = {F.option_type : F.CE, F.price : 0, F.loop_no : 1, F.stratagy : F.FS_Fourth+"_"+F.Hedges, F.exit_percent : 100, F.qty : qty, F.transaction_type : F.Buy, F.broker_name : broker_name, F.broker_session : broker_session})
            pe_thread = Thread(name = f'PE_{F.Hedges}_{i}_Thread', target = order_placer, kwargs = {F.option_type : F.PE, F.price : 0, F.loop_no : 1, F.stratagy : F.FS_Fourth+"_"+F.Hedges, F.exit_percent : 100, F.qty : qty, F.transaction_type : F.Buy, F.broker_name : broker_name, F.broker_session : broker_session})
            ce_thread.start()
            pe_thread.start()
            ce_thread.join()
            pe_thread.join()
        for i in range(env.qty_partation_loop):
            # i = 2
            ce_thread = Thread(name = f'CE_{F.FS_Fourth}_{i}_Thread', target=order_placer, kwargs = {F.option_type: F.CE, F.price : 30, F.loop_no : i, F.stratagy : F.FS_Fourth, F.exit_percent : 20, F.qty : qty, F.transaction_type : F.Sell, F.broker_name : broker_name, F.broker_session : broker_session, 'wait_percent' : 5})
            pe_thread = Thread(name = f'PE_{F.FS_Fourth}_{i}_Thread', target=order_placer, kwargs = {F.option_type: F.PE, F.price : 30, F.loop_no : i, F.stratagy : F.FS_Fourth, F.exit_percent : 20, F.qty : qty, F.transaction_type : F.Sell, F.broker_name : broker_name, F.broker_session : broker_session, 'wait_percent' : 5})
            ce_thread.start()
            pe_thread.start()
            ce_thread.join()
            pe_thread.join()
            
    elif current_time >= env.FS_Fifth and not is_expiry:
        if is_expiry :
            ce_thread = Thread(name = f'CE_{F.Hedges}_{i}_Thread', target = order_placer, kwargs = {F.option_type : F.CE, F.price : 0, F.loop_no : 1, F.stratagy : F.FS_Fifth+"_"+F.Hedges, F.exit_percent : 100, F.qty : qty, F.transaction_type : F.Buy, F.broker_name : broker_name, F.broker_session : broker_session})
            pe_thread = Thread(name = f'PE_{F.Hedges}_{i}_Thread', target = order_placer, kwargs = {F.option_type : F.PE, F.price : 0, F.loop_no : 1, F.stratagy : F.FS_Fifth+"_"+F.Hedges, F.exit_percent : 100, F.qty : qty, F.transaction_type : F.Buy, F.broker_name : broker_name, F.broker_session : broker_session})
            ce_thread.start()
            pe_thread.start()
            ce_thread.join()
            pe_thread.join()
        for i in range(env.qty_partation_loop):
            ce_thread = Thread(name = f'CE_{F.FS_Fifth}_{i}_Thread',target=order_placer, kwargs = {F.option_type : F.CE, F.price : 30 , F.loop_no : i, F.stratagy : F.FS_Fifth, F.exit_percent : 20, F.qty : qty, F.transaction_type : F.Sell, F.broker_name : broker_name, F.broker_session : broker_session, 'wait_percent' : 5})
            pe_thread = Thread(name = f'PE_{F.FS_Fifth}_{i}_Thread',target=order_placer, kwargs = {F.option_type : F.PE, F.price : 30 , F.loop_no : i, F.stratagy : F.FS_Fifth, F.exit_percent : 20, F.qty : qty, F.transaction_type : F.Sell, F.broker_name : broker_name, F.broker_session : broker_session, 'wait_percent' : 5})
            ce_thread.start()
            pe_thread.start()
            ce_thread.join()
            pe_thread.join()
            
    elif current_time >= env.RB_Buy_first and is_expiry:
        for i in range(env.qty_partation_loop):
            # i = 2
            ce_thread = Thread(name = f'CE_{F.RB_Buy_first}_{i}_Thread',target=order_placer, kwargs = {F.option_type : F.CE, F.price : 0, F.loop_no : i, F.stratagy : F.RB_Buy_first, F.exit_percent : 20, F.qty : qty, F.transaction_type : F.Buy, F.broker_name : broker_name, F.broker_session : broker_session, 'wait_percent' : 5})
            pe_thread = Thread(name = f'PE_{F.RB_Buy_first}_{i}_Thread',target=order_placer, kwargs = {F.option_type : F.PE, F.price : 0, F.loop_no : i, F.stratagy : F.RB_Buy_first, F.exit_percent : 20, F.qty : qty, F.transaction_type : F.Buy, F.broker_name : broker_name, F.broker_session : broker_session, 'wait_percent' : 5})
            ce_thread.start()
            pe_thread.start()
            ce_thread.join()
            pe_thread.join()
            
    elif current_time >= env.exit_orders and is_straragy_traded(all_closed=True):
        ce_thread = Thread(name = 'CE_exit_orders-Thread',target = day_end, kwargs={F.option_type: F.CE, F.broker_name : broker_name, F.broker_session : broker_session})
        pe_thread = Thread(name = 'PE_exit_orders-Thread',target = day_end, kwargs={F.option_type: F.PE, F.broker_name : broker_name, F.broker_session : broker_session})
        ce_thread.start()
        pe_thread.start()
        ce_thread.join()
        pe_thread.join()

    elif current_time == env.logout_session:
        Order_management(broker_name,broker_session).Update_Performance()




if __name__ == '__main__':

    while True:
        date = dt.today().date()
        if not env.env_variable_initilised or (env.today != date) and is_market_time() :
            is_env = env.load_env_variable()
            broker_name = env.broker_name
            hoilyday, holiday_reason = is_hoilyday()
            if not hoilyday:
                is_login, broker_session = Login().setup()
                start_socket_thread = Thread(name = 'socket_thread', target = socket_thread_fun, kwargs = {'broker_session': broker_session,'broker_name' : broker_name})
                env.thread_list.append(start_socket_thread)
                start_socket_thread.start()
                # send_message(message = "Buddy... I'm here :)",emergency= True)
                
        if is_market_time() and not hoilyday and env.option_chain_set:
            current_time = get_ist_now()
            is_socket_open = env.socket_open
             
            placing(current_time = current_time, broker_name = broker_name, broker_session = broker_session)

            if is_socket_open:
                Checking(broker_session,broker_name).interval_check()
                
                    
            if not is_socket_open :
                start_socket_thread = Thread(name = 'socket_thread_restart', target = socket_thread_fun, kwargs = {'broker_session': broker_session,'broker_name' : broker_name})
                send_message(message = 'Socket Re-started', emergency = True)
                env.thread_list.append(start_socket_thread)
                start_socket_thread.start()
                
            while wait_until_next_minute() > 8 :
                t_one = time.time() 
                Checking(broker_session,broker_name).continue_check()
                t_two = time.time() 
                # print(f'wait_until_next_minute : {wait_until_next_minute()} Checking Time : {t_two - t_one}')
                time.sleep(2)
            else:
                time.sleep(wait_until_next_minute())
                
                                  
        if not is_market_time() or hoilyday:
            
            if hoilyday:
                send_message(message = f"Buddy... It's Holiday : {holiday_reason}\nBye...", emergency = True)
                sleep_till_next_day()
                
            elif not is_market_time():
                for i in env.thread_list :
                    i.join()
                option_chain.clear()
                ticker_to_token.clear()
                for thread in env.thread_list:
                    thread.join()
                send_message(message = "I'm tired going to sleep ;)", emergency = True)
                sleep_till_next_day()
                
        
            
