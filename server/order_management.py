from server.execuations import OrderExecuation
from server.utils import send_message
from  datetime import datetime as dt
import pandas as pd
import time
import json
from server.broker import Order_details, get_ltp, get_token, is_order_rejected_func
from server.utils import env_variables as env ,database , Fields as F
import plotly.express as px
pd.options.mode.chained_assignment = None # it is mandatory
import os


class Order_management : 
    def __init__(self, broker_name, broker_session):
        self.broker_name = broker_name
        self.broker_session = broker_session
        self.time = str(dt.now())
        self.date = dt.today().date()
        self.database = database()
        self.database = database()

    def order_place(self,ticker,qty, transaction_type, stratagy, exit_percent, loop_no, price, option_type,):
        tag = f'{stratagy}_{option_type}_{loop_no}'
        if stratagy == F.FS_First:
            price = round(price, 1)  
            trigger_price = price + 0.05
                
            is_order_placed, order_number, product_type, tag = OrderExecuation(self.broker_name, self.broker_session).place_order(price, trigger_price, qty, ticker, transaction_type, tag)
            if is_order_placed :       
                time.sleep(3)
                is_not_empty,all_orders,filled_order,pending_order = Order_details(self.broker_session,self.broker_name).order_book()
                all_filled_orders = pending_order[F.order_id].to_list() + filled_order[F.order_id].to_list()
                if order_number in all_filled_orders:
                    order = { 
                            F.entry_time : self.time,
                            F.ticker : ticker,
                            F.token : get_token(ticker),
                            F.transaction_type : transaction_type,
                            F.product_type : product_type,
                            F.option_type : option_type,
                            F.qty : qty,
                            #-------------- Entry order details -------------
                            F.entry_orderid : order_number,
                            F.entry_orderid_status  : F.open,    #To check order is complete
                            F.entry_price : price,
                            F.entry_price_initial : price,
                            F.entry_tag : tag,  #tag_contains stratagy_name+option_type+loop no
                            F.entry_order_count : 0,
                            F.entry_order_execuation_type : None,
                            #-------------- sl order details -------------
                            F.exit_orderid : None,
                            F.exit_orderid_status : None,
                            F.exit_price : 0,
                            F.exit_price_initial : 0,
                            F.exit_tag : None,  #tag_contains stratagy_name+option_type+loop no
                            #-------- Other parameter --------------
                            F.exit_price:0,
                            F.exit_time: None,
                            F.exit_reason : None,              # sl_hit/day_end
                            F.exit_order_count : 0,
                            F.exit_order_execuation_type : None,
                            F.stratagy : stratagy,
                            F.index : env.index,
                            F.loop_no : loop_no,
                            F.exit_percent : exit_percent,
                            F.charges : 0,
                            F.drift_points : 0,
                            F.drift_rs : 0,
                            F.pl : 0,
                            F.recording: [
                                # {'Time':'10:15:00','pl':100},
                               ]
                            }

                    self.database[str(self.date)].insert_one(order)
                    send_message(message = f"limit order placed... \nStratagy : {stratagy}\nPrice : {price}\nOption Type : {option_type}\nProduct Type : {product_type}\nMessage : {order_number}",stratagy = stratagy)
                    self.smart_executer(stratagy = stratagy, exit_percent = exit_percent, option_type = option_type, entry_orderid = order_number )


            elif not is_order_placed :
                send_message(message = f'Not able to place {stratagy} order \nmessage : {order_number}',emergency = True)


        if stratagy == F.Hedges :
            price = round(price, 1) # remoe in production 
            trigger_price = price - 0.05
                
            is_order_placed, order_number, product_type, tag = OrderExecuation(self.broker_name, self.broker_session).place_order(price, trigger_price, qty, ticker, transaction_type, tag)
            # print(is_order_placed, order_number, product_type, tag)
            if is_order_placed :       
                time.sleep(3)
                is_not_empty,all_orders,filled_order,pending_order = Order_details(self.broker_session,self.broker_name).order_book()
                all_filled_orders = pending_order[F.order_id].to_list() + filled_order[F.order_id].to_list()
                if order_number in all_filled_orders:
                    order = { 
                            F.entry_time : self.time,
                            F.ticker : ticker,
                            F.token : get_token(ticker),
                            F.transaction_type : transaction_type,
                            F.product_type : product_type,
                            F.option_type : option_type,
                            F.qty : qty,
                            #-------------- Entry order details -------------
                            F.entry_orderid : order_number,
                            F.entry_orderid_status  : F.open,    #To check order is complete
                            F.entry_price : price,
                            F.entry_price_initial : price,
                            F.entry_tag : tag,  #tag_contains stratagy_name+option_type+loop no
                            F.entry_order_count : 0,
                            F.entry_order_execuation_type : None,
                            #-------------- sl order details -------------
                            F.exit_orderid : None,
                            F.exit_orderid_status : None,
                            F.exit_price : 0,
                            F.exit_price_initial : 0,
                            F.exit_tag : None,  #tag_contains stratagy_name+option_type+loop no
                            #-------- Other parameter --------------
                            F.exit_price:0,
                            F.exit_time: None,
                            F.exit_reason : None,              # sl_hit/day_end
                            F.exit_order_count : 0,
                            F.exit_order_execuation_type : None,
                            F.stratagy : stratagy,
                            F.index : env.index,
                            F.loop_no : loop_no,
                            F.exit_percent : exit_percent,
                            F.charges : 0,
                            F.drift_points : 0,
                            F.drift_rs : 0,
                            F.pl : 0,
                            F.recording: [
                                # {'Time':'10:15:00','pl':100},
                               ]
                            }

                    self.database[str(self.date)].insert_one(order)
                    send_message(message = f"limit order placed... \nStratagy : {stratagy}\nPrice : {price}\nOption Type : {option_type}\nProduct Type : {product_type}\nMessage : {order_number}",stratagy = stratagy)
                    self.smart_executer(stratagy = stratagy, exit_percent = exit_percent, option_type = option_type, entry_orderid = order_number )


            elif not is_order_placed :
                send_message(message = f'Not able to place {stratagy} order \nmessage : {order_number}',emergency = True)


        if stratagy in [F.RE_First, F.RE_Second, F.RE_Third]:
            trigger_price = price + 0.05
            
            is_order_placed, order_number, product_type, tag = OrderExecuation(self.broker_name, self.broker_session).place_order(price, trigger_price, qty, ticker, transaction_type, tag)
            if is_order_placed :  
                time.sleep(3)
                is_not_empty,all_orders,filled_order,pending_order = Order_details(self.broker_session,self.broker_name).order_book()
                all_filled_orders = pending_order[F.order_id].to_list() + filled_order[F.order_id].to_list()
                if order_number in all_filled_orders:
                    order = {
                            F.entry_time : self.time,
                            F.ticker : ticker,
                            F.token : get_token(ticker),
                            F.transaction_type : transaction_type,
                            F.product_type : product_type,
                            F.option_type : option_type,
                            F.qty: qty,
                            #-------------- Entry order details -------------
                            F.entry_orderid : order_number,
                            F.entry_orderid_status  : F.open,    #To check order is complete
                            F.entry_price : price,
                            F.entry_price_initial : price,
                            F.entry_order_count : 0,
                            F.entry_tag : tag,  #tag_contains stratagy_name+option_type+loop 
                            F.entry_order_execuation_type: None,
                            #-------------- sl order details -------------
                            F.exit_orderid : None,
                            F.exit_orderid_status : None,
                            F.exit_price : 0,
                            F.exit_price_initial : 0,
                            F.exit_tag : None,  #tag_contains stratagy_name+option_type+loop no
                            F.exit_order_count : 0,
                            F.exit_order_execuation_type : None ,
                            #-------- Other parameter --------------
                            F.exit_price : 0,
                            F.exit_time : None,
                            F.exit_reason : None,              # sl_hit/day_end
                            F.stratagy : stratagy,
                            F.index : env.index,
                            F.loop_no : loop_no,
                            F.exit_percent : exit_percent,
                            F.charges : 0,
                            F.drift_points : 0,
                            F.drift_rs : 0,
                            F.pl : 0,
                            F.recording: [
                                # {'Time':'10:15:00','pl':100},
                               ]
                            }

                    self.database[str(self.date)].insert_one(order)
                    send_message(message = f"limit order placed... \nStratagy : {stratagy}\nPrice : {price}\nOption Type : {option_type}\nMessage : {order_number}", stratagy = stratagy)
                    self.smart_executer(stratagy=stratagy, exit_percent=exit_percent, option_type=option_type, entry_orderid = order_number)
            elif not is_order_placed :
                    send_message(message = f'Not able to place {stratagy} order \nmessage : {order_number}', emergency = True)

        if stratagy == F.WNT_First:
            price = round(price * 0.95,1)
            trigger_price = price + 0.05
            
            is_order_placed, order_number, product_type, tag  = OrderExecuation(self.broker_name, self.broker_session).place_order(price, trigger_price, qty,ticker, transaction_type, tag)
            if is_order_placed :  
                time.sleep(3)
                is_not_empty,all_orders,filled_order,pending_order = Order_details(self.broker_session,self.broker_name).order_book()
                all_filled_orders = pending_order[F.order_id].to_list()+filled_order[F.order_id].to_list()
                if order_number in all_filled_orders:
                    order = { 
                            F.entry_time : self.time,
                            F.ticker : ticker,
                            F.token : get_token(ticker),
                            "ltp": 0,
                            F.transaction_type : transaction_type,
                            F.option_type : option_type,
                            F.product_type : product_type,
                            F.qty : qty,
                            #-------------- Entry order details -------------
                            F.entry_orderid : order_number,
                            F.entry_orderid_status  : F.open,    #To check order is complete
                            F.entry_price : price,
                            F.entry_price_initial : price,
                            F.entry_tag : tag,  #tag_contains stratagy_name+option_type+loop no
                            F.entry_order_count : 0,
                            F.entry_order_execuation_type : None,
                            #-------------- sl order details -------------
                            F.exit_orderid : None,
                            F.exit_orderid_status : None,
                            F.exit_price : 0,
                            F.exit_price_initial : 0,
                            F.exit_tag : None,  #tag_contains stratagy_name+option_type+loop no
                            #-------- Other parameter --------------
                            F.exit_price : 0,
                            F.exit_time : None,
                            F.exit_reason : None,              # sl_hit/day_end
                            F.exit_order_count : 0,
                            F.exit_order_execuation_type : None,
                            F.stratagy : stratagy,
                            F.index : env.index,
                            F.loop_no : loop_no,
                            F.exit_percent : exit_percent,
                            F.charges : 0,
                            F.drift_points : 0,
                            F.drift_rs : 0,
                            F.pl : 0,
                            F.recording : [
                                # {'Time':'10:15:00','pl':100},
                               ]
                            }

                    self.database[str(self.date)].insert_one(order)
                    send_message(message = f"limit order placed... \nStratagy : {stratagy}\nPrice : {price}\nOption Type : {option_type}\nMessage : {order_number}", stratagy= stratagy)

            elif not is_order_placed :
                send_message(message = f'Not able to place {stratagy} order \nmessage :{order_number}', emergency = True)

    def smart_executer(self, stratagy, exit_percent, option_type, entry_orderid) :
        while True:
            order_details = Order_details(self.broker_session,self.broker_name)
            is_not_empty, all_orders, filled_order, pending_order = order_details.order_book()
            myquery = {F.entry_orderid: { "$eq": entry_orderid }, F.entry_orderid_status: { "$eq": F.open }}
            db_data = self.database[str(self.date)].find(myquery)
            pending_orders_db = pd.DataFrame(db_data)
            if len(pending_orders_db) != 0 :
                for index,row in pending_orders_db.iterrows():
                    count = row[F.entry_order_count]
                    if (row[F.entry_orderid] not in pending_order[F.order_id].tolist()) and (row[F.entry_orderid_status] == F.open) :
                        entry_price = filled_order[filled_order[F.order_id] == row[F.entry_orderid]].iloc[0][F.price]
                        if row[F.entry_order_execuation_type] == None : 
                            self.database[str(self.date)].update_one({F.entry_orderid : row[F.entry_orderid]}, { "$set": {F.entry_time : self.time, F.entry_price : float(entry_price), F.entry_orderid_status: F.closed, F.entry_order_execuation_type : F.limit_order}}) # it will update if sl hit  modificarion status to slhit
                            send_message(message = f'Placed in broker end \nOrder id : {row[F.entry_orderid]}\nOption Type : {row[F.option_type]}\nExecuation Type : {F.limit_order}\nCount : {count}\nStratagy : {row[F.stratagy]}', stratagy = row[F.stratagy])
                            # time.sleep(5)
                        else : 
                            self.database[str(self.date)].update_one({F.entry_orderid : row[F.entry_orderid]}, { "$set": {F.entry_time : self.time, F.entry_price : float(entry_price), F.entry_orderid_status: F.closed}}) # it will update if sl hit  modificarion status to slhit
                            send_message(message = f'Placed in broker end \nOrder id : {row[F.entry_orderid]}\nOption Type : {row[F.option_type]}\nExecuation Type : {row[F.entry_order_execuation_type]}\nCount : {count}\nStratagy : {row[F.stratagy]}', stratagy = row[F.stratagy])
                            # time.sleep(5)
                        
                    elif (count < 3) and (row[F.entry_orderid_status] == F.open) :
                        ltp = get_ltp(row[F.token],self.broker_name)
                        price = row[F.entry_price] 

                        if ltp>price:
                            new_price = round(abs(price) + abs(ltp-price)/2,1)
                        else:
                            new_price = round(abs(price) - abs(ltp-price)/2,1)
                        # send_message(message = f" Modification of : {row[F.entry_orderid]}\nltp :{ltp} old_price:{price} new_price :{new_price} count : {count}\n\n")
                        try : 
                            remaning_qty = pending_order[pending_order[F.order_id] == row[F.entry_orderid]].iloc[0][F.qty]
                            is_modified, order_number, message = OrderExecuation(self.broker_name,self.broker_session).modify_order(order_id = row[F.entry_orderid], new_price = new_price ,quantity = remaning_qty)
                            is_order_rejected, is_mis_blocked = is_order_rejected_func(order_number,self.broker_session,self.broker_name)
                            if is_modified and not is_order_rejected:
                                self.database[str(self.date)].update_one({F.entry_orderid : row[F.entry_orderid]}, { "$set": {F.entry_price : new_price , F.entry_order_count: count + 1 } }) # it will update updated price to database
                                send_message(message = f"Entry price modified \nMessage :{order_number} order modified\nOld Price : {price} New Price : {new_price} \nRemaning Qty : {remaning_qty}\nSide : {row[F.option_type]}\nStratagy : {row[F.stratagy]}", stratagy = row[F.stratagy])
                                
                            elif not is_modified :
                                send_message(message = f'Not able to modify sl in Smart Execuator(kotak.modify_order)\nMessage : {message}\nStratagy : {row[F.stratagy]}\nSide : {row[F.option_type]}\nOrder Id : {order_number}', emergency = True)
                                    
                        except Exception as e : 
                            send_message(message = f'Not able to modify sl in Smart Execuator(kotak.modify_order)\nMessage : {e}\nStratagy : {row[F.stratagy]}\nSide : {row[F.option_type]}\nOrder Id : {order_number}', emergency = True)

                    else:
                        remaning_qty = pending_order[pending_order[F.order_id] == row[F.entry_orderid]].iloc[0][F.qty]
                        is_modified, order_number, message = OrderExecuation(self.broker_name,self.broker_session).modify_order(order_id = row[F.entry_orderid], new_price = 0 ,quantity = remaning_qty, trigger_price = 0, order_type = "MKT")
                        is_order_rejected, is_mis_blocked = is_order_rejected_func(order_number,self.broker_session,self.broker_name)
                        if is_modified and not is_order_rejected:
                            self.database[str(self.date)].update_one({F.entry_orderid : order_number}, { "$set": {F.entry_order_execuation_type : F.market_order, F.entry_order_count : count + 1} })
                            #  send_message(message = f"Executed at Market Order \nMessage :{order_number} \nRemaning Qty : {remaning_qty}\nSide : {row[F.option_type]}\nStratagy : {row[F.option_type]}", stratagy = row[F.stratagy])
                        elif not is_modified :
                            send_message(message = f'Not able to modify sl in Smart Executer SOD Market Order\nMessage : {message}\nStratagy : {row[F.stratagy]}\nSide : {row[F.option_type]}\nOrder Id : {order_number}', emergency = True)
                            
                    time.sleep(3)


            else:
                if stratagy != F.Hedges :
                    myquery = {F.entry_orderid: { "$eq": entry_orderid}}
                    pending_orders_db = self.database[str(self.date)].find(myquery)
                    for i in pending_orders_db:
                        # print(f"ticker={i[F.ticker]},qty={i[F.qty]},transaction_type={i[F.transaction_type]},avg_price={(i[F.entry_price])},exit_percent={exit_percent},tag = {tag}")
                        self.place_limit_sl(ticker = i[F.ticker], qty = i[F.qty], transaction_type_ = i[F.transaction_type], avg_price = (i[F.entry_price]), exit_percent = i[F.exit_percent], option_type = i[F.option_type],stratagy = stratagy, tag = i[F.entry_tag])
                    break
                
                else : 
                    break

    def place_limit_sl(self,ticker,qty,transaction_type_,avg_price,exit_percent,option_type,stratagy,tag):
        stoploos = round(round(0.05 * round(avg_price / 0.05), 2) * ((100 + exit_percent) / 100), 1)
        if transaction_type_ == F.Buy:
            transaction_type = F.Sell

        if transaction_type_ == F.Sell:
            transaction_type = F.Buy
        if stratagy == F.Hedges : 
            trigger_price = round(stoploos + 0.05,2) # 0.05 is the diffrance betweeen limit price and trigger price
        if stratagy != F.Hedges : 
            trigger_price = round(stoploos - 0.05,2) # 0.05 is the diffrance betweeen limit price and trigger price
        is_order_placed, order_number, product_type, new_tag = OrderExecuation(self.broker_name,self.broker_session).place_order(price = stoploos, trigger_price = trigger_price , qty = qty, ticker = ticker , transaction_type = transaction_type, tag = tag + '_sl')
        if is_order_placed :
            self.database[str(self.date)].update_one({ "entry_tag" : tag}, { "$set" : {F.exit_orderid : order_number, F.exit_orderid_status : F.open , F.exit_price : stoploos, F.exit_price_initial : stoploos, F.exit_tag : tag + '_sl'}})
            send_message(message = f"Sl order placed... \nOrder Number : {order_number}\nPrice : {stoploos}\nSide : {option_type}\nStratagy : {stratagy}", stratagy = stratagy)

        elif not is_order_placed:
            self.database[str(self.date)].update_one({ "entry_tag": tag}, { "$set": {F.exit_orderid_status : F.rejected }})
            send_message(message = f'Problem in placing limit_sl\nMessage : {order_number}', emergency = True)

    def exit_orders_dayend(self,option_type) :
        hedges_sl_placed = False
        while True:
            myquery = {F.option_type:{'$eq':option_type},'$or': [{F.exit_orderid_status : F.open},{F.exit_orderid_status : F.re_entry_open}]}
            db_data = self.database[str(self.date)].find(myquery)
            pending_orders_db = pd.DataFrame(db_data)
            
            
            order_details = Order_details(self.broker_session,self.broker_name)
            is_not_empty,all_orders,filled_order,pending_order = order_details.order_book()

            if len(pending_orders_db) != 0:
                for index,row in pending_orders_db.iterrows():
                    count = row[F.exit_order_count]
                    if (row[F.exit_orderid]  not in pending_order[F.order_id].tolist()) and (row[F.exit_orderid_status] in [F.open,F.re_entry_open]) :
                        exit_price = filled_order[filled_order[F.order_id] == row[F.exit_orderid]].iloc[0][F.price]
                        if row[F.exit_order_execuation_type] == None : 
                            self.database[str(self.date)].update_one({F.exit_orderid : row[F.exit_orderid]}, { "$set": {F.exit_orderid_status : F.closed, F.exit_reason : F.day_end, F.exit_price : float(exit_price) , F.exit_price_initial : float(exit_price), F.exit_time : self.time, F.exit_order_execuation_type : F.limit_order} } )
                            send_message(message = f'Trade Closed... \nOrder id : {row[F.exit_orderid]}\nPrice : {exit_price}\nOption Type : {row[F.option_type]}\nExecuation Type : {F.limit_order}\nCount : {row[F.exit_order_count]}\nStratagy : {row[F.stratagy]}', stratagy = row[F.stratagy])
                        else : 
                            self.database[str(self.date)].update_one({F.exit_orderid : row[F.exit_orderid]}, { "$set": {F.exit_orderid_status : F.closed, F.exit_reason : F.day_end, F.exit_price : float(exit_price) , F.exit_price_initial : float(exit_price), F.exit_time : self.time} } )
                            send_message(message = f'Trade Closed... \nOrder id : {row[F.exit_orderid]}\nOption Type : {row[F.option_type]}\nExecuation Type : {row[F.exit_order_execuation_type]}\nCount : {row[F.exit_order_count]}\nStratagy : {row[F.stratagy]}', stratagy = row[F.stratagy])
                    elif count < 3:
                        ltp = get_ltp(row[F.token],self.broker_name)
                        # ltp = 40
                        price = row[F.exit_price] 
                        ## code for the modification to mean price  
                        # if ltp > price:
                        #     new_price = round(abs(price) + abs(ltp - price)/2 ,1)
                        # else:
                        #     new_price = round(abs(price) - abs(ltp - price)/2 ,1)
                        # print(f"ltp :{ltp} old_price:{price} new_price :{new_price} count : {count}\n\n")
                        try : 
                            remaning_qty = pending_order[pending_order[F.order_id] == row[F.exit_orderid]].iloc[0][F.qty]
                            is_modified, order_number, message = OrderExecuation(self.broker_name,self.broker_session).modify_order(order_id = row[F.exit_orderid], new_price = ltp ,quantity = remaning_qty)
                            if is_modified :
                                if count == 0 :
                                    self.database[str(self.date)].update_one({F.entry_orderid : row[F.entry_orderid]}, {"$set": {F.exit_price_initial : ltp, F.exit_price : ltp , F.exit_order_count : count + 1 }}) # it will update updated price to database
                                    send_message(message = f"Exit price modified \nMessage :{order_number}\nOld Price : {price} New Price : {ltp} \nRemaning Qty : {remaning_qty}\nStratagy : {row[F.stratagy]}", stratagy = row[F.stratagy])
                                else : 
                                    self.database[str(self.date)].update_one({F.entry_orderid : row[F.entry_orderid]}, {"$set": {F.exit_price : ltp , F.exit_order_count: count + 1 }}) # it will update updated price to database
                                    send_message(message = f"Exit price modified \nMessage :{order_number}\nOld Price : {price} New Price : {ltp} \nRemaning Qty : {remaning_qty}\nStratagy : {row[F.stratagy]}", stratagy = row[F.stratagy])
                                    
                            elif not is_modified :
                                send_message(message = f'Not able to modify sl in exit_orders_dayend(kotak.modify_order)\nStratagy : {row[F.stratagy]}\nMessage : {message}\nOrder Id : {order_number}', emergency = True)
                        except Exception as e : 
                           send_message(message = f'Not able to modify sl in exit_orders_dayend(kotak.modify_order)\nMessage : {e}\nStratagy : {row[F.stratagy]}\nSide : {row[F.option_type]}\nOrder Id : {order_number}', emergency = True)
                        
                    else:
                        remaning_qty = pending_order[pending_order[F.order_id] == row[F.exit_orderid]].iloc[0][F.qty]
                        is_modified, order_number, message = OrderExecuation(self.broker_name, self.broker_session).modify_order(order_id = row[F.exit_orderid], new_price = 0, quantity = remaning_qty, trigger_price = 0, order_type = "MKT")
                        if is_modified :
                            # send_message(message = f"Executed at Market Order \nMessage :{order_number}")
                            self.database[str(self.date)].update_one({F.exit_orderid : row[F.exit_orderid]}, {"$set": {F.exit_order_execuation_type : F.market_order, F.exit_order_count : count + 1 }})
                        elif not is_modified:
                            send_message(message = f'Not able to modify sl in exit_orders_dayend() Market Order\nMessage : {message}\nOrder Id : {order_number}', emergency = True)
                time.sleep(3)
            
            elif not hedges_sl_placed :               # Remove Hedges Here
                myquery = {F.stratagy : { "$eq" : F.Hedges}, F.option_type : {'$eq' : option_type}}
                pending_orders_db = self.database[str(self.date)].find(myquery)
                for i in pending_orders_db:
                    # print(f"ticker={i[F.ticker]},qty={i[F.qty]},transaction_type={i[F.transaction_type]},avg_price={(i[F.entry_price])},exit_percent={exit_percent},tag = {tag}")
                    self.place_limit_sl(ticker = i[F.ticker], qty = i[F.qty], transaction_type_ = i[F.transaction_type], avg_price = (i[F.entry_price]), exit_percent = i[F.exit_percent], option_type = i[F.option_type], stratagy = i[F.stratagy], tag = i[F.entry_tag])
                
                hedges_sl_placed = True
            
            else:
                myquery = {F.option_type:{'$eq' : option_type},'$or': [{F.entry_orderid_status : F.open},{F.entry_orderid_status : F.re_entry_open}]}
                db_data = self.database[str(self.date)].find(myquery)
                for i in db_data:
                    is_canceled,order_number, message = OrderExecuation(self.broker_name,self.broker_session).cancel_order(i[F.entry_orderid])
                    if is_canceled : 
                        self.database[str(self.date)].update_one({F.entry_orderid : i[F.entry_orderid]}, {"$set": {F.exit_reason : F.day_end}}) 
                        send_message(message = f"Order Cancel... \nMessage : {order_number}\nStratagy : {i[F.stratagy]}", stratagy = i[F.stratagy])
                    else : 
                        send_message(f'Not able to cancle order {i[F.exit_orderid]} in cancel_pending_order()\n Reason : {message}', emergency = True)
                        
                break
     
    def Update_Performance(self):         
        # Update calculation to database
        try :
            self.genrate_plot()
            stratagy_df = pd.DataFrame({F.stratagy: [F.FS_First, F.RE_First, F.WNT_First, F.RE_Second, F.RE_Third, F.Hedges]})
            df = pd.DataFrame(self.database[str(self.date)].find())
            df_stratagy_cal = df[[F.stratagy,F.pl,F.drift_points,F.drift_rs,F.entry_order_count,F.exit_order_count,F.index]]
            df_stratagy_cal = df_stratagy_cal.groupby(F.stratagy).agg({F.pl : 'sum', F.drift_points : 'sum', F.drift_rs : 'sum',F.entry_order_count : 'sum',F.exit_order_count : 'sum' ,F.index : 'count' }).reset_index()
            df_stratagy_cal = stratagy_df.merge(df_stratagy_cal,on = F.stratagy, how = 'left')
            df_stratagy_cal['total_count'] = df_stratagy_cal[F.entry_order_count] + df_stratagy_cal[F.exit_order_count]

            # Total Calculation for the day
            total_pl = round(df_stratagy_cal[F.pl].sum(),2)
            total_drift_points = round(df_stratagy_cal[F.drift_points].sum(),2)
            total_drift_rs = round(df_stratagy_cal[F.drift_rs].sum(),2)
            total_modifications = df_stratagy_cal['total_count'].sum()
            total_orders = len(df)

            message  = f'Instrument : {env.index}\nTotal PL : {total_pl}\nTotal Drift-Points : {total_drift_points}\nTotal Drift in RS : {total_drift_rs}\nTotal Orders : {total_orders}\nTotal Modifications : {total_modifications}\n{25 * "-"}\nStratagy Wise Report :\n{25 * "-"}\n'
            for index,row in df_stratagy_cal.iterrows():
                message += (f'Strtatagy : {row[F.stratagy]}\nPL : {row["pl"]}\nDrift in Points : {round(row[F.drift_points],2)}\nDrift in RS : {row[F.drift_rs]}\nOrders : {row[F.index]}\nModifications  : {(row["total_count"])}\n{25 * "-"}\n')

            # Update to Performance Tracker
            column_sums = df_stratagy_cal.sum()
            df_stratagy_cal.loc[len(df_stratagy_cal)] = column_sums
            df_stratagy_cal.rename(columns = {'index' : 'no_orders'},inplace=True)
            df_stratagy_cal = df_stratagy_cal.replace({df_stratagy_cal.iloc[-1].stratagy : 'Total'})
            df_stratagy_cal['date'] = str(dt.today().date())
            df_stratagy_cal['index'] = env.index
            df_stratagy_cal = df_stratagy_cal[['date','index','stratagy', 'pl', 'drift_points', 'drift_rs', 'entry_order_count','exit_order_count', 'no_orders', 'total_count']]
            data = json.loads(df_stratagy_cal.to_json(orient='records'))
            database(day_tracker = True)[dt.now().strftime('%b')].insert_many(data)
            send_message(message = message)
        except Exception as e : 
            send_message(message = f'Problem in Update_Performance PL\nMessage : {e}', emergency = True)
            
    def genrate_plot(self):
        db = database(recording=True)[str(self.date)].find()
        df = pd.DataFrame(db)[['Time',F.pl,F.free_margin]]
        df['Time'] = pd.to_datetime(df['Time'],format='%Y-%m-%d %H:%M:%S')
        df.set_index('Time',inplace=True)
        heading_dict = {F.pl : "PL Recording",F.free_margin : "Available Margin"}
        color_dict = {F.pl : '#ace385',F.free_margin : "#85d0e3"}
        for col in df.columns : 
            fig = px.area(df, x = df.index, y= col,title=f"<b>{heading_dict[col]} ({self.date})</b>",width = 1500,height = 720,labels = {"Time": "Time","value": col})
            fig.update_layout(
                            title_font_size=35,
                            title_font_family="Times New Roman",
                            title_x=0.5,
                            font=dict(
                                    family="Arial",
                                    size=15
                                ) )
            
            fig.update_traces(fillcolor = color_dict[col],line=dict(color='black', width=2))
            # fig.show()
            if col == F.pl : 
                folder_path = "server/plots/PL_Recording"
            elif col == F.free_margin : 
                folder_path = "server/plots/Available_Margin"
                
            os.makedirs(folder_path, exist_ok=True)
            image_full_path = f'{folder_path}/{self.date} {heading_dict[col]}.png'
            fig.write_image(image_full_path)
            send_message(message = image_full_path,send_image=True)
            
