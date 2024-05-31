from execuations import OrderExecuation
from telegram_bot import logger_bot,emergency_bot
from  datetime import datetime as dt
import pandas as pd
import time
from broker import Order_details, get_ltp, get_token, is_order_rejected_func
from utils import env_variables as env ,get_db, Fields as F

pd.options.mode.chained_assignment = None # it is mandatory


class Order_management : 
    def __init__(self,broker_name,broker_session):
        self.broker_name = broker_name
        self.broker_session = broker_session
        self.entry_id = get_db()
        self.date = dt.today().date()
        

    def order_place(self,ticker,qty,transaction_type,stratagy,exit_percent,loop_no,price,option_type,):
        tag = f'{stratagy}_{option_type}_{loop_no}'
        if stratagy == F.NineTwenty:
            price = round(price,1) # remoe in production 
            trigger_price = price+0.05
            is_order_placed,order_number,message = OrderExecuation(self.broker_name, self.broker_session).place_order(price,trigger_price,qty,ticker,transaction_type,tag)
            is_order_rejected = is_order_rejected_func(order_number,self.broker_session,self.broker_name)
            if is_order_placed and not is_order_rejected :       
                time.sleep(5)
                is_not_empty,all_orders,filled_order,pending_order = order_details =  Order_details(self.broker_session,self.broker_name).order_book()
                all_filled_orders = pending_order[F.order_id].to_list()+filled_order[F.order_id].to_list()
                
                # print('pending_order',pending_order)
                # print('all_orders',all_orders[F.order_id]['order_id'])
                if order_number in all_filled_orders:
                    order = { 
                            F.entry_time:str(dt.now()),
                            F.ticker  : ticker,
                            F.token : get_token(ticker),
                            F.transaction_type : transaction_type,
                            F.option_type : option_type,
                            F.qty: qty,
                            #-------------- Entry order details -------------
                            F.entry_orderid : order_number,
                            F.entry_orderid_status  : F.open,    #To check order is complete
                            F.entry_price : price,
                            F.entry_price_initial: price,
                            F.entry_tag : tag,  #tag_contains stratagy_name+option_type+loop no
                            F.entry_order_count : 0,
                            F.entry_order_execuation_type : None,
                            #-------------- sl order details -------------
                            F.exit_orderid :'---',
                            F.exit_orderid_status :'---',
                            F.exit_price:0,
                            F.exit_price_initial:0,
                            F.exit_tag: '---',  #tag_contains stratagy_name+option_type+loop no
                            #-------- Other parameter --------------
                            F.exit_price:0,
                            F.exit_time:'---',
                            F.exit_reason : '---',              # sl_hit/day_end
                            F.exit_order_count : 0,
                            F.exit_order_execuation_type : None,
                            F.stratagy : stratagy,
                            F.loop_no:loop_no,
                            F.exit_percent:exit_percent,
                            F.charges : 0,
                            F.drift_points : 0,
                            F.drift_rs : 0,
                            F.recording: [
                                # {'Time':'10:15:00','pl':100},
                               ]
                            }

                    self.entry_id [str(self.date )].insert_one(order)
                    logger_bot(f"{stratagy} {option_type} limit order placed SucessFully !!! \nMessage : {order_number}")
                    self.smart_executer(stratagy=stratagy,exit_percent=exit_percent,option_type=option_type )


            elif not is_order_placed :
                emergency_bot(f'Not able to palce {stratagy} order \nmessage : {message}')

        if stratagy in [F.NineThirty, F.TenThirty, F.Eleven]:
            trigger_price = price+0.05
            
            is_order_placed, order_number,message  = OrderExecuation(self.broker_name, self.broker_session).place_order(price,trigger_price,qty,ticker,transaction_type,tag)
            is_order_rejected = is_order_rejected_func(order_number,self.broker_session,self.broker_name)
            if is_order_placed and not is_order_rejected :  
                time.sleep(5)
                is_not_empty,all_orders,filled_order,pending_order = order_details =  Order_details(self.broker_session,self.broker_name).order_book()
                all_filled_orders = pending_order[F.order_id].to_list()+filled_order[F.order_id].to_list()
                if order_number in all_filled_orders:
                    order = {
                            F.entry_time:str(dt.now()),
                            F.ticker  : ticker,
                            F.token : get_token(ticker),
                            F.transaction_type : transaction_type,
                            F.option_type : option_type,
                            F.qty: qty,
                            #-------------- Entry order details -------------
                            F.entry_orderid : order_number,
                            F.entry_orderid_status  : F.open,    #To check order is complete
                            F.entry_price : price,
                            F.entry_price_initial: price,
                            F.entry_order_count : 0,
                            F.entry_tag : tag,  #tag_contains stratagy_name+option_type+loop 
                            F.entry_order_execuation_type: None,
                            #-------------- sl order details -------------
                            F.exit_orderid :'---',
                            F.exit_orderid_status :'---',
                            F.exit_price:0,
                            F.exit_price_initial:0,
                            F.exit_tag: '',  #tag_contains stratagy_name+option_type+loop no
                            F.exit_order_count : 0,
                            F.exit_order_execuation_type : None ,
                            #-------- Other parameter --------------
                            F.exit_price:0,
                            F.exit_time:'---',
                            F.exit_reason : '---',              # sl_hit/day_end
                            F.stratagy : stratagy,
                            F.loop_no : loop_no,
                            F.exit_percent : exit_percent,
                            F.charges : 0,
                            F.drift_points : 0,
                            F.drift_rs : 0,
                            F.recording: [
                                # {'Time':'10:15:00','pl':100},
                               ]
                            }

                    self.entry_id [str(self.date )].insert_one(order)
                    logger_bot(f"{stratagy} {option_type} limit order placed SucessFully !!! \nMessage : {order_number}")
                    self.smart_executer(stratagy=stratagy,exit_percent=exit_percent,option_type=option_type)
            elif not is_order_placed :
                    emergency_bot(f'Not able to palce {stratagy} order \nmessage : {message}')

        if stratagy == F.NineFourtyFive:
            price=round(price*0.95,1)
            trigger_price = price+0.05
            
            is_order_placed, order_number, message  = OrderExecuation(self.broker_name, self.broker_session).place_order(price,trigger_price,qty,ticker,transaction_type,tag)
            is_order_rejected = is_order_rejected_func(order_number,self.broker_session,self.broker_name)
            if is_order_placed and not is_order_rejected :  
                time.sleep(5)
                is_not_empty,all_orders,filled_order,pending_order = order_details =  Order_details(self.broker_session,self.broker_name).order_book()
                all_filled_orders = pending_order[F.order_id].to_list()+filled_order[F.order_id].to_list()
                if order_number in all_filled_orders:
                    order = { 
                            F.entry_time:str(dt.now()),
                            F.ticker  : ticker,
                            F.token : get_token(ticker),
                            "ltp": 0,
                            F.transaction_type : transaction_type,
                            F.option_type : option_type,
                            F.qty: qty,
                            #-------------- Entry order details -------------
                            F.entry_orderid : order_number,
                            F.entry_orderid_status  : F.open,    #To check order is complete
                            F.entry_price : price,
                            F.entry_price_initial: price,
                            F.entry_tag : tag,  #tag_contains stratagy_name+option_type+loop no
                            F.entry_order_count : 0,
                            F.entry_order_execuation_type : None,
                            #-------------- sl order details -------------
                            F.exit_orderid :'',
                            F.exit_orderid_status :'',
                            F.exit_price:0,
                            F.exit_price_initial:0,
                            F.exit_tag: '',  #tag_contains stratagy_name+option_type+loop no
                            #-------- Other parameter --------------
                            F.exit_price:0,
                            F.exit_time:'',
                            F.exit_reason : '',              # sl_hit/day_end
                            F.exit_order_count : 0,
                            F.exit_order_execuation_type : None,
                            F.stratagy : stratagy,
                            F.loop_no:loop_no,
                            F.exit_percent:exit_percent,
                            F.charges : 0,
                            F.drift_points : 0,
                            F.drift_rs : 0,
                            F.recording: [
                                # {'Time':'10:15:00','pl':100},
                               ]
                            }

                    self.entry_id [str(self.date )].insert_one(order)
                    logger_bot(f"{stratagy} {option_type} limit order placed SucessFully !!! \nMessage : {order_number}")

            elif not is_order_placed :
                emergency_bot(f'Not able to palce {stratagy} order \nmessage :{message}')


    def smart_executer(self,stratagy,exit_percent,option_type) :
        while True:
            order_details =  Order_details(self.broker_session,self.broker_name)
            is_not_empty,all_orders,filled_order,pending_order = order_details.order_book()
            myquery = {F.stratagy: { "$eq": stratagy }, F.option_type: { "$eq": option_type } , F.entry_orderid_status: {"$eq":  F.open }}
            db_data = self.entry_id [str(self.date )].find(myquery)
            pending_orders_db = pd.DataFrame(db_data)
            if len(pending_orders_db)!=0:
                print("New loop started\n")
                for index,row in pending_orders_db.iterrows():
                    count = row[F.entry_order_count]
                    if (row[F.entry_orderid]  not in pending_order[F.order_id].tolist()):
                        self.entry_id [str(self.date )].update_one({F.entry_orderid : row[F.entry_orderid]}, { "$set": {F.entry_orderid_status: F.closed, F.entry_order_execuation_type : F.limit_order, F.entry_order_count : count+1 } }) # it will update if sl hit  modificarion status to slhit
                        logger_bot(f'Placed in broker end \nOrder id : {row[F.entry_orderid]} \nOption Type : {row[F.option_type]}')
                        time.sleep(1)
                        continue
                    elif count<5:
                        ltp = get_ltp(row[F.token],self.broker_name)
                        # ltp = 40
                        price = row[F.entry_price] 

                        if ltp>price:
                            new_price  =round(abs(price)+abs(ltp-price)/2,1)
                        else:
                            new_price  =round(abs(price)-abs(ltp-price)/2,1)
                        logger_bot(f" Modification of  : {row[F.entry_orderid]}\nltp :{ltp} old_price:{price} new_price :{new_price} count : {count}\n\n")
                        # print("pending_tag :\n ",pending_order[F.order_id])
                        # print("F.entry_tag : ",row[F.entry_orderid] )
                        # print("pending_orders_orderid :\n ",pending_order[pending_order[F.order_id]==row[F.entry_orderid]])
                        
                        remaning_qty = pending_order[pending_order[F.order_id]==row[F.entry_orderid]].iloc[0][F.qty]
                        is_modified, order_number, message = OrderExecuation(self.broker_name,self.broker_session).modify_order(order_id = row[F.entry_orderid], new_price =new_price ,quantity = remaning_qty)
                        is_order_rejected = is_order_rejected_func(order_number,self.broker_session,self.broker_name)
                        if is_modified and not is_order_rejected:
                            self.entry_id [str(self.date )].update_one({F.entry_orderid : row[F.entry_orderid]}, { "$set": {  F.entry_price : new_price , F.entry_order_count: count + 1 } }) # it will update updated price to database
                            logger_bot(f"Entry price modified \nMessage :{order_number} order modified\nOld Price : {price} New Price : {new_price} \nRemaning Qty : {remaning_qty}")
                            
                        elif not is_modified :
                            emergency_bot(f'Not able to modify sl in Smart Execuator(kotak.modify_order)\nMessage : {message}')

                    else:
                        remaning_qty = pending_order[pending_order[F.order_id]==row[F.entry_orderid]].iloc[0][F.qty]
                        is_modified, order_number, message  = (self.broker_name,self.broker_session).modify_order(order_id = row[F.entry_orderid], new_price =0 ,quantity = remaning_qty,trigger_price =0, order_type = "MKT")
                        is_order_rejected = is_order_rejected_func(order_number,self.broker_session,self.broker_name)
                        if is_modified and not is_order_rejected:
                            
                            order_details =  Order_details(self.broker_session,self.broker_name)
                            is_not_empty,all_orders,filled_order,pending_order = order_details.order_book()
                            market_execute_price = filled_order[filled_order[F.order_id] == order_number].iloc[0][F.price]
                            print( f"Final market price : ", market_execute_price )
                            
                            self.entry_id [str(self.date )].update_one({F.entry_orderid : order_number}, { "$set": {F.entry_price : float(market_execute_price) , F.entry_order_count: count + 1 , F.entry_order_execuation_type : F.market_order} })
                            logger_bot(f"Executed at Market Order \nMessage :{order_number} \nRemaning Qty : {remaning_qty}")
                        elif not is_modified :
                            emergency_bot(f'Not able to modify sl in Smart Executer SOD Market Order\nMessage : {message}')
                            
                    time.sleep(5)


            else:
                myquery = {F.entry_orderid_status: {"$eq":  F.closed},F.stratagy: { "$eq": stratagy }, F.option_type: { "$eq": option_type }, F.exit_orderid: { "$eq": '---' }}
                pending_orders_db = self.entry_id [str(self.date )].find(myquery)
                for i in pending_orders_db:
                    tag = f'{i[F.stratagy]}_{i[F.option_type]}_{i[F.loop_no]}'
                    # print(f"ticker={i[F.ticker]},qty={i[F.qty]},transaction_type={i[F.transaction_type]},avg_price={(i[F.entry_price])},exit_percent={exit_percent},tag = {tag}")
                    self.place_limit_sl(ticker=i[F.ticker],qty=i[F.qty],transaction_type_=i[F.transaction_type],avg_price=(i[F.entry_price]),exit_percent=exit_percent,option_type= i[F.option_type],tag = tag)
                break

    def place_limit_sl(self,ticker,qty,transaction_type_,avg_price,exit_percent,option_type,tag):
        if transaction_type_ == F.Buy:
            transaction_type =  F.Sell

        elif transaction_type_ == F.Sell:
            transaction_type =  F.Buy
        else:
            pass
        
        stoploos = round(round(0.05*round(avg_price/0.05),2)*((100+exit_percent)/100),1)
        trigger_price =round(stoploos-0.05,2) # 0.05 is the diffrance betweeen limit price and trigger price
        sl_placed,order_number, message  = OrderExecuation(self.broker_name,self.broker_session).place_order(price = stoploos, trigger_price = trigger_price , qty= qty, ticker= ticker , transaction_type = transaction_type, tag = tag+'_sl')
        
        is_order_rejected = is_order_rejected_func(order_number,self.broker_session,self.broker_name)
        if sl_placed and not is_order_rejected:
            self.entry_id [str(self.date )].update_one({ "entry_tag": tag}, { "$set": {F.exit_orderid : order_number,  F.exit_orderid_status :  F.open , F.exit_price : stoploos, F.exit_price_initial : stoploos, F.exit_tag: tag+'_sl'} } )
            logger_bot(f"Sl order palced Sucessfully !!! \nOrder Number : {order_number}\nPrice : {stoploos}\nSide : {option_type}")

        elif not sl_placed:
            self.entry_id [str(self.date )].update_one({ "entry_tag": tag}, { "$set": {F.exit_orderid_status :  F.rejected }})
            emergency_bot(f'Problem in palcing limit_sl\nMessage : {message}')

    def exit_orders_dayend(self,option_type) :
       
        while True:
            myquery = {F.option_type:{'$eq':option_type},'$or': [{F.exit_orderid_status : F.open},{F.exit_orderid_status : F.re_entry_open}]}
            db_data = self.entry_id [str(self.date )].find(myquery)
            pending_orders_db = pd.DataFrame(db_data)
            
            order_details =  Order_details(self.broker_session,self.broker_name)
            is_not_empty,all_orders,filled_order,pending_order = order_details.order_book()

            if len(pending_orders_db) != 0:
                for index,row in pending_orders_db.iterrows():
                    count = row[F.exit_order_count]
                    if (row[F.exit_orderid]  not in pending_order[F.order_id].tolist()):
                        exit_price = filled_order[filled_order[F.order_id] == row[F.exit_orderid]].iloc[0][F.price]
                        self.entry_id [str(self.date )].update_one({F.exit_orderid : row[F.exit_orderid]}, { "$set": {F.exit_orderid_status : F.closed, F.exit_reason : F.day_end, F.exit_price:float(exit_price), F.exit_time:str(dt.now()), F.exit_order_execuation_type : F.limit_order, F.exit_order_count : count+1} } )

                    elif count<5:
                        ltp = get_ltp(row[F.token],self.broker_name)
                        # ltp = 40
                        price = row[F.exit_price] 
                        ## code for the modification to mean price
                        # if ltp > price:
                        #     new_price = round(abs(price) + abs(ltp - price)/2 ,1)
                        # else:
                        #     new_price = round(abs(price) - abs(ltp - price)/2 ,1)
                        # print(f"ltp :{ltp} old_price:{price} new_price :{new_price} count : {count}\n\n")
                        
                        remaning_qty = pending_order[pending_order[F.order_id]==row[F.exit_orderid]].iloc[0][F.qty]
                        is_modified, order_number, message = OrderExecuation(self.broker_name,self.broker_session).modify_order(order_id = row[F.exit_orderid], new_price = ltp ,quantity = remaning_qty)
                        if is_modified :
                            if count == 0 :
                                self.entry_id [str(self.date )].update_one({F.entry_orderid : row[F.entry_orderid]}, {"$set": {F.exit_price_initial : ltp, F.exit_price : ltp , F.exit_order_count: count + 1 }}) # it will update updated price to database
                                logger_bot(f"Exit price modified \nMessage :{order_number} order modified\nOld Price : {price} New Price : {ltp} \nRemaning Qty : {remaning_qty}")
                            else : 
                                self.entry_id [str(self.date )].update_one({F.entry_orderid : row[F.entry_orderid]}, {"$set": {F.exit_price : ltp , F.exit_order_count: count + 1 }}) # it will update updated price to database
                                logger_bot(f"Exit price modified \nMessage :{order_number} order modified\nOld Price : {price} New Price : {ltp} \nRemaning Qty : {remaning_qty}")
                                
                        elif not is_modified :
                            emergency_bot(f'Not able to modify sl in exit_orders_dayend(kotak.modify_order)\nMessage : {message}')
                        
                    else:
                        remaning_qty = pending_order[pending_order[F.order_id]==row[F.exit_orderid]].iloc[0][F.qty]
                        
                        is_modified, order_number, message  = OrderExecuation(self.broker_name,self.broker_session).modify_order(order_id = row[F.exit_orderid], new_price = 0, quantity =remaning_qty,trigger_price = 0, order_type = "MKT")
                        if is_modified :
                            logger_bot(f"Executed at Market Order \nMessage :{order_number}")
                            self.entry_id [str(self.date )].update_one({F.exit_orderid : row[F.exit_orderid]}, {"$set": {F.exit_order_execuation_type : F.market_order }})
                        elif not is_modified:
                            emergency_bot(f'Not able to modify sl in exit_orders_dayend() Market Order\nMessage : {message}')
                time.sleep(5)
            else:
                myquery = {F.option_type:{'$eq' : option_type},'$or': [{F.entry_orderid_status : F.open},{F.entry_orderid_status : F.re_entry_open}]}
                db_data = self.entry_id [str(self.date )].find(myquery)
                for i in db_data:
                    is_canceled,order_number, message  = OrderExecuation(self.broker_name,self.broker_session).cancel_order(i[F.entry_orderid])
                    if is_canceled : 
                        logger_bot(f"pending order \nMessage :{order_number} is canceled")
                    else : 
                        emergency_bot(f'Not able to cancle order {i[F.exit_orderid]} in cancel_pending_order()\n Reason : {message}')
                        
                break
