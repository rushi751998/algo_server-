
from server.broker import Login, get_symbol, Order_details, Socket_handling, option_chain, ticker_to_token
from server.checking import Checking
from server.utils import (get_ist_now,
                   is_hoilyday,
                   is_market_time,
                   sleep_till_next_day,
                   env_variables as env ,
                   database , Fields as F,
                   get_qty_option_price)

from server.utils import emergency_bot, logger_bot
from server.order_management import Order_management
from neo_api_client import NeoAPI
import time,re,pymongo,os ,pandas as pd 
from threading import Thread
import threading
from  datetime import datetime as dt
pd.options.mode.chained_assignment = None # it is mandatory

from random import randint


def socket_thread_fun(**kwargs):
    Socket_handling(broker_name,broker_session).start_socket(kwargs['expiry_base_instrument'])


def order_placer(option_type,option_price,loop_no,stratagy,exit_percent,qty,transaction_type,broker_name,broker_session,wait_percent = None):
    ticker,ltp = get_symbol(option_type,option_price,broker_name)
    price = round(ltp * ((100-wait_percent)/100),1) if wait_percent != None else ltp
    Order_management(broker_name,broker_session).order_place(ticker,qty = qty,transaction_type = transaction_type, stratagy = stratagy, exit_percent = exit_percent, loop_no = loop_no ,price = price, option_type = option_type)
    
def day_end(broker_name,broker_session,option_type):
    Order_management(broker_name,broker_session).exit_orders_dayend(option_type)

    
def placing(current_time, broker_name, broker_session):
    # current_time = kwargs['current_time']
    fifty_per_qty, fifty_per_price, re_entry_qty, re_entry_price, wait_trade_qty, wait_trade_price = get_qty_option_price(broker_name)
    # print(env.index,re_entry_qty, re_entry_price)
    
    if current_time == env.NineTwenty:
        for i in range(env.qty_partation_loop):
            i = 4
            ce_thread = Thread(name = f'CE_{F.NineTwenty}', target = order_placer, kwargs = {F.option_type : F.CE, 'option_price' : fifty_per_price, F.loop_no : i, F.stratagy : F.NineTwenty, F.exit_percent : 50, F.qty : fifty_per_qty, F.transaction_type : F.Sell, F.broker_name : broker_name, F.broker_session : broker_session})
            pe_thread = Thread(name = f'PE_{F.NineTwenty}', target = order_placer, kwargs = {F.option_type : F.PE, 'option_price' : fifty_per_price, F.loop_no : i, F.stratagy : F.NineTwenty, F.exit_percent : 50, F.qty : fifty_per_qty, F.transaction_type : F.Sell, F.broker_name : broker_name, F.broker_session : broker_session})
            env.thread_list.append(ce_thread)
            env.thread_list.append(pe_thread)
            ce_thread.start()
            pe_thread.start()

    elif current_time == env.NineThirty:
        for i in range(env.qty_partation_loop):
            i = 12
            ce_thread = Thread(name = f'CE_{F.NineThirty}', target = order_placer, kwargs = {F.option_type : F.CE, 'option_price' : re_entry_price, F.loop_no : i, F.stratagy : F.NineThirty, F.exit_percent : 20, F.qty : re_entry_qty, F.transaction_type : F.Sell, F.broker_name : broker_name, F.broker_session : broker_session})
            pe_thread = Thread(name = f'PE_{F.NineThirty}', target = order_placer, kwargs = {F.option_type : F.PE, 'option_price' : re_entry_price, F.loop_no : i, F.stratagy : F.NineThirty, F.exit_percent : 20, F.qty : re_entry_qty, F.transaction_type : F.Sell, F.broker_name : broker_name, F.broker_session : broker_session})
            env.thread_list.append(ce_thread)
            env.thread_list.append(pe_thread)
            ce_thread.start()
            pe_thread.start()
            
    elif current_time == env.NineFourtyFive:
        for i in range(env.qty_partation_loop):
            i = 4
            ce_thread = Thread(name = f'CE_{F.NineFourtyFive}-Thread', target = order_placer, kwargs = {F.option_type: F.CE, 'option_price' : wait_trade_price, F.loop_no : i, F.stratagy : F.NineFourtyFive, F.exit_percent : 50, F.qty : wait_trade_qty, F.transaction_type : F.Sell, F.broker_name : broker_name, F.broker_session : broker_session, 'wait_percent' : 5})
            pe_thread = Thread(name = f'PE_{F.NineFourtyFive}-Thread', target = order_placer, kwargs = {F.option_type: F.PE, 'option_price' : wait_trade_price, F.loop_no : i, F.stratagy : F.NineFourtyFive, F.exit_percent : 50, F.qty : wait_trade_qty, F.transaction_type : F.Sell, F.broker_name : broker_name, F.broker_session : broker_session, 'wait_percent' : 5})
            env.thread_list.append(ce_thread)
            env.thread_list.append(pe_thread)
            ce_thread.start()
            pe_thread.start()
            
    elif current_time == env.TenThirty:
        for i in range(env.qty_partation_loop):
            i = 4
            ce_thread = Thread(name = f'CE_{F.TenThirty}-Thread', target=order_placer, kwargs = {F.option_type: F.CE, 'option_price' : re_entry_price, F.loop_no : i, F.stratagy : F.TenThirty, F.exit_percent : 20, F.qty : re_entry_qty, F.transaction_type : F.Sell, F.broker_name : broker_name, F.broker_session : broker_session})
            pe_thread = Thread(name = f'PE_{F.TenThirty}-Thread', target=order_placer, kwargs = {F.option_type: F.PE, 'option_price' : re_entry_price, F.loop_no : i, F.stratagy : F.TenThirty, F.exit_percent : 20, F.qty : re_entry_qty, F.transaction_type : F.Sell, F.broker_name : broker_name, F.broker_session : broker_session})
            env.thread_list.append(ce_thread)
            env.thread_list.append(pe_thread)
            ce_thread.start()
            pe_thread.start()
            
    elif current_time == env.Eleven:
        for i in range(env.qty_partation_loop):
            i = 4
            ce_thread = Thread(name = f'CE_{F.Eleven}-Thread',target=order_placer, kwargs = {F.option_type : F.CE, 'option_price' : re_entry_price, F.loop_no : i, F.stratagy : F.Eleven, F.exit_percent : 20, F.qty : re_entry_qty, F.transaction_type : F.Sell, F.broker_name : broker_name, F.broker_session : broker_session})
            pe_thread = Thread(name = f'PE_{F.Eleven}-Thread',target=order_placer, kwargs = {F.option_type : F.PE, 'option_price' : re_entry_price, F.loop_no : i, F.stratagy : F.Eleven, F.exit_percent : 20, F.qty : re_entry_qty, F.transaction_type : F.Sell, F.broker_name : broker_name, F.broker_session : broker_session})
            env.thread_list.append(ce_thread)
            env.thread_list.append(pe_thread)
            ce_thread.start()
            pe_thread.start()

    elif current_time == env.exit_orders:
        ce_thread = Thread(name = 'CE_exit_orders-Thread',target = day_end, kwargs={F.option_type: F.CE, F.broker_name : broker_name, F.broker_session : broker_session})
        pe_thread = Thread(name = 'PE_exit_orders-Thread',target = day_end, kwargs={F.option_type: F.PE, F.broker_name : broker_name, F.broker_session : broker_session})
        env.thread_list.append(ce_thread)
        env.thread_list.append(pe_thread)
        ce_thread.start()
        pe_thread.start()

    elif current_time == env.logout_session:
        for thread in env.thread_list:
            thread.join()
        Order_management(broker_name,broker_session).calculate_pl()




if __name__ == '__main__':

    while True:
        date = dt.today().date()
        start_time = get_ist_now().second
        if not env.env_variable_initilised or (env.today != date):
            is_env = env.load_env_variable()
            event_list = [env.login, env.NineTwenty, env.NineTwenty, env.NineThirty, env.NineFourtyFive, env.TenThirty, env.Eleven, env.exit_orders, env.logout_session]
            broker_name = env.broker_name
            hoilyday, holiday_reason = is_hoilyday()
            if not hoilyday:
                is_login, broker_session = Login().setup()
                start_socket_thread = Thread(name = 'socket_thread', target = socket_thread_fun, kwargs = {'expiry_base_instrument' : env.expiry_base_instrument,'broker_session': broker_session,'broker_name' : broker_name})
                env.thread_list.append(start_socket_thread)
                start_socket_thread.start()
                # logger_bot('Socket thread Started')
        # print(env.option_chain_set)
        if is_market_time() and not hoilyday and env.option_chain_set:
            current_time = dt.strftime(get_ist_now(), '%H:%M')
            is_socket_open = env.socket_open
            # print(is_socket_open)
             
            if is_socket_open and (current_time in event_list ):
                placing(current_time = current_time, broker_name = broker_name, broker_session = broker_session)

            if is_socket_open:
                Checking(broker_session,broker_name).check()
                time.sleep(60 - (get_ist_now().second - start_time))
                
            if not is_socket_open : 
                start_socket_thread = Thread(name = 'socket_thread_restart', target = socket_thread_fun, kwargs = {'expiry_base_instrument' : env.expiry_base_instrument,'broker_session': broker_session,'broker_name' : broker_name})
                emergency_bot('Socket thread Re-started')
                env.thread_list.append(start_socket_thread)
                start_socket_thread.start()
                                  
        if not is_market_time() or hoilyday:
            
            if hoilyday:
                emergency_bot(f"Today is holiday because: {holiday_reason}\nCatch You Tomorrow... !!")
                sleep_till_next_day()
                
            elif not is_market_time():
                for i in env.thread_list :
                    i.join()
                option_chain.clear()
                ticker_to_token.clear()
                emergency_bot("Market is Stopped, Catch You Tomorrow... !!")
                sleep_till_next_day()
