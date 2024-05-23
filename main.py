
from broker import Login, get_symbol, Order_details, Socket_handling
from checking import Checking
from utils import (get_ist_now,
                   is_hoilyday,
                   is_market_time,
                   sleep_till_next_day,
                   env_variables as env ,
                   get_db, Fields as F)

from telegram_bot import emergency_bot
from order_management import Order_management
from neo_api_client import NeoAPI
import time,re,pymongo,os ,pandas as pd 
from threading import Thread
from dateutil.relativedelta import relativedelta
from datetime import timedelta
from  datetime import datetime as dt
pd.options.mode.chained_assignment = None # it is mandatory

from random import randint

login = dt.strftime(get_ist_now(),'%H:%M')
first_order = dt.strftime(get_ist_now(),'%H:%M')
second_order = '18:18'
third_order = '18:20'
fourth_order = '18:20'
fifth_order = '16:20'
exit_orders = '15:01'
logout_session = '15:15'



def order_placer(option_type,option_price,loop_no,stratagy,exit_percent,transaction_type,broker_name,broker_session):
                ticker,ltp = get_symbol(option_type,option_price,broker_name)
                print("ticker,ltp : ",ticker,ltp)
                Order_management(broker_name,broker_session).order_place(ticker,qty=15,transaction_type=transaction_type,stratagy=stratagy,exit_percent=exit_percent,loop_no=loop_no,price=ltp,option_type=option_type)


# def placing(current_time_,broker_name,broker_session):
#     kwargs={'current_time':current_time,F.broker_name:broker_name,F.broker_session:broker_session}
    
def placing(**kwargs):
    current_time = kwargs['current_time']

    if current_time==first_order:
        for i in range(1):
            i = 15
            
            ce_thread = Thread(target=order_placer,kwargs={ F.option_type: F.CE,'option_price' :150,F.loop_no : i, F.stratagy :  F.NineTwenty,F.exit_percent : 50, F.transaction_type :  F.Sell,F.broker_name:broker_name,F.broker_session : broker_session})
            pe_thread = Thread(target=order_placer,kwargs={ F.option_type: F.PE,'option_price' :150,F.loop_no : i, F.stratagy :  F.NineTwenty,F.exit_percent : 50, F.transaction_type :  F.Sell,F.broker_name:broker_name,F.broker_session : broker_session})
            env.thread_listlistlist.append(ce_thread)
            env.thread_list.append(pe_thread)
            ce_thread.start()
            pe_thread.start()
            # ce_thread = order_placer(option_type= F.CE,option_price =150,loop_no = i,stratagy =  F.NineTwenty,exit_percent = 50,transaction_type  =  F.Sell, broker_name = kwargs[F.broker_name], broker_session = kwargs[F.broker_session])
            # pe_thread = order_placer(option_type= F.PE,option_price =150,loop_no = i,stratagy =  F.NineTwenty,exit_percentnt = 50,transaction_type  =  F.Sell, broker_name = kwargs[F.broker_name], broker_session = kwargs[F.broker_session])

    elif current_time==second_order:
        for i in range(1):
            ce_thread = Thread(target=order_placer,kwargs={ F.option_type: F.CE,'option_price' :125,F.loop_no : i, F.stratagy :  F.NineThirty,F.exit_percent : 20, F.transaction_type :  F.Sell,F.broker_name:kwargs[F.broker_name],F.broker_session : kwargs[F.broker_session]})
            pe_thread = Thread(target=order_placer,kwargs={ F.option_type: F.PE,'option_price' :125,F.loop_no : i, F.stratagy :  F.NineThirty,F.exit_percent : 20, F.transaction_type :  F.Sell,F.broker_name:kwargs[F.broker_name],F.broker_session : kwargs[F.broker_session]})
            env.thread_list.append(ce_thread)
            env.thread_list.append(pe_thread)
            ce_thread.start()
            pe_thread.start()
            
    elif current_time==third_order:
        for i in range(1):
            ce_thread = Thread(target=order_placer,kwargs={ F.option_type: F.CE,'option_price' :100,F.loop_no : i, F.stratagy :  F.NineFourtyFive,F.exit_percent : 20, F.transaction_type :  F.Sell,F.broker_name:kwargs[F.broker_name],F.broker_session : kwargs[F.broker_session]})
            pe_thread = Thread(target=order_placer,kwargs={ F.option_type: F.PE,'option_price' :100,F.loop_no : i, F.stratagy :  F.NineFourtyFive,F.exit_percent : 20, F.transaction_type :  F.Sell,F.broker_name:kwargs[F.broker_name],F.broker_session : kwargs[F.broker_session]})
            env.thread_list.append(ce_thread)
            env.thread_list.append(pe_thread)
            ce_thread.start()
            pe_thread.start()
            
    elif current_time== fourth_order:
        for i in range(1):
            ce_thread = Thread(target=order_placer,kwargs={ F.option_type: F.CE,'option_price' :125,F.loop_no : i, F.stratagy :  F.TenThirty,F.exit_percent : 20, F.transaction_type :  F.Sell,F.broker_name:kwargs[F.broker_name],F.broker_session : kwargs[F.broker_session]})
            pe_thread = Thread(target=order_placer,kwargs={ F.option_type: F.PE,'option_price' :125,F.loop_no : i, F.stratagy :  F.TenThirty,F.exit_percent : 20, F.transaction_type :  F.Sell,F.broker_name:kwargs[F.broker_name],F.broker_session : kwargs[F.broker_session]})
            env.thread_list.append(ce_thread)
            env.thread_list.append(pe_thread)
            ce_thread.start()
            pe_thread.start()
            
    elif current_time== fifth_order:
        for i in range(1):
            ce_thread = Thread(target=order_placer,kwargs={ F.option_type: F.CE,'option_price' :125,F.loop_no : i, F.stratagy :  F.Eleven, F.exit_percent : 20, F.transaction_type :  F.Sell,F.broker_name:kwargs[F.broker_name],F.broker_session : kwargs[F.broker_session]})
            pe_thread = Thread(target=order_placer,kwargs={ F.option_type: F.PE,'option_price' :125,F.loop_no : i, F.stratagy :  F.Eleven, F.exit_percent : 20, F.transaction_type :  F.Sell,F.broker_name:kwargs[F.broker_name],F.broker_session : kwargs[F.broker_session]})
            env.thread_list.append(ce_thread)
            env.thread_list.append(pe_thread)
            ce_thread.start()
            pe_thread.start()

    elif current_time==exit_orders:
        cancel_pending_order()
        ce_thread = Thread(target=Order_management(broker_name,broker_session).exit_orders_dayend,args=(F.CE))
        pe_thread = Thread(target=Order_management(broker_name,broker_session).exit_orders_dayend,args=(F.PE))
        env.thread_list.append(ce_thread)
        env.thread_list.append(pe_thread)
        ce_thread.start()
        pe_thread.start()

    elif current_time==logout_session:
        for thread in env.thread_list:
            thread.join()
            
        pass

    else:
        print(f"not getting time : {current_time}\n")
    wait_to_align = 60.0 - start_time
    time.sleep(wait_to_align)



if __name__ == '__main__':
    
    # is_env = env.load_env_variable()
    # broker_name = env.broker_name
    # start_time = get_ist_now().second
    # current_time = dt.strftime(get_ist_now(),'%H:%M')

    # if current_time == login : 
        
    #     future_token,broker_session = Login().setup()
    #     broker_name = env.broker_name


    #     if current_time == login : 
            
    #         placing_thread = placing(current_time,broker_name,broker_session)

            
        
            
    while True:
        date = dt.today().date()
        
        if not env.env_variable_initilised and (env.today != date):
            is_env = env.load_env_variable()
            hoilyday,holiday_reason = is_hoilyday()
            event_list = [first_order,second_order,third_order,fourth_order,fifth_order,exit_orders,logout_session]
            broker_name = env.broker_name
            is_login,broker_session = Login().setup()
            
        else : 
            start_time = get_ist_now().second
            current_time = dt.strftime(get_ist_now(),'%H:%M')
            
            if current_time in event_list: 
                if is_market_time() and not hoilyday :
                    if is_login:
                        start_socket_thread = Thread(name='socket_thread',target=Socket_handling(F.kotak_neo,broker_session).start_socket()) 
                        if not start_socket_thread.is_alive() :
                            env.thread_list.append(start_socket_thread)       
                            start_socket_thread.start()
                            time.sleep(10)      
                        else : 
                            placing_thread = Thread(target=placing, kwargs={'current_time':current_time,F.broker_name:broker_name,F.broker_session:broker_session})
                            env.thread_list.append(placing_thread)
                            # placing_thread.start()
                    else : 
                        is_login,broker_session = Login().setup()
                else : 
                    emergency_bot(f"Today is holiday beacause : {holiday_reason}")
                    sleep_till_next_day()  
                    # time.sleep(5)
                    pass
            else : 
                if is_market_time() and not hoilyday:
                    checking_thread = Thread(target=Checking(broker_session=broker_session,broker_name=broker_name).check())
                    checking_thread.start()
                    time.sleep(60 - (get_ist_now().second - start_time))
                else :
                    sleep_till_next_day()

            
            
            
            
            
    
    
    
    
    
    
    
    
    
    
    
    # while True:
    #     start_time = get_ist_now().second
    #     current_time = dt.strftime(get_ist_now(),'%H:%M')
    #     event_list = [first_order,second_order,third_order,exit_order,logout]
    #     broker_name = os.environ[F.broker_name]


    #     if current_time == login : 
    #         broker_session,future_token,client= Login(broker_name).setup()
    #         socket_thread = Thread(target=Login(broker_name).start(client))
    #         socket_thread = (Login(broker_name).start(client))
            # socket_thread.start()
            
         
        # if current_time == event_list : 
        #     placing_thread = Thread(target=placing,kwargs={'current_time' :current_time,F.broker_name:broker_name,F.broker_session:broker_session})
        #     placing_thread.start()
            
        # else : 
        #     checking_thread = Thread(target=checking,kwargs = {'current_time' :current_time,F.broker_name:broker_name,F.broker_session:broker_session})
        #     checking_thread.start()
        #     time.sleep(60 - (get_ist_now().second - start_time))
            

