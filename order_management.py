from execuations import OrderExecuation
from telegram_bot import logger_bot,emergency_bot,alert_bot
from  datetime import datetime as dt
import pandas as pd
import time
from broker import Order_details, get_ltp, get_token
from utils import env_variables as env ,get_db, Fields as F

pd.options.mode.chained_assignment = None # it is mandatory


class Order_management : 
    def __init__(self,broker_name,broker_session):
        self.broker_name = broker_name
        self.broker_session = broker_session
        self.entry_id = get_db()
        self.date = dt.today().date()
        

    def order_place(self,ticker,qty,transaction_type,stratagy,sl_percent,loop_no,price,option_type,):
        tag = f'{stratagy}_{option_type}_{loop_no}'
        if stratagy == F.NineTwenty:
            price = round(price*1.2,1) # remoe in production 
            trigger_price = price+0.05
            is_order_placed,order_number = OrderExecuation(self.broker_name, self.broker_session).place_order(price,trigger_price,qty,ticker,transaction_type,tag)       
            # print(is_order_placed,order_number)        
            if is_order_placed : 
                time.sleep(5)
                all_orders,filled_order,pending_order = order_details =  Order_details(self.broker_session,self.broker_name).order_book()
                all_filled_orders = pending_order[F.order_id].to_list()+filled_order[F.order_id].to_list()
                
                # print('pending_order',pending_order)
                # print('all_orders',all_orders[F.order_id]['order_id'])
                if order_number in all_filled_orders:
                    print(True)
                    order = { 
                            F.entry_time:str(dt.now()),
                            F.ticker  : ticker,
                            F.token : get_token(ticker),
                            F.transaction_type : transaction_type,
                            F.option_type : option_type,
                            F.qty: qty,
                            #-------------- Entry order details -------------
                            F.entry_orderid : order_number,
                            F.entry_orderid_status  : F.pending_order,    #To check order is complete
                            F.entry_price : price,
                            F.entry_price_initial: price,
                            F.entry_tag : tag,  #tag_contains stratagy_name+option_type+loop no
                            #-------------- sl order details -------------
                            F.sl_orderid :'---',
                            F.sl_orderid_status :'---',
                            F.sl_price:0,
                            F.sl_price_initial:0,
                            F.sl_tag: '---',  #tag_contains stratagy_name+option_type+loop no
                            #-------- Other parameter --------------
                            F.exit_price:0,
                            F.exit_time:'---',
                            F.exit_reason : '---',              # sl_hit/day_end
                            F.stratagy : stratagy,
                            F.loop_no:loop_no,
                            F.sl_percent:sl_percent,
                            F.charges : 0,
                            F.drift_points : 0,
                            F.drift_rs : 0,
                            F.recording: [
                                # {'Time':'10:15:00','pl':100},
                                ]
                            }

                    self.entry_id [str(self.date )].insert_one(order)
                    logger_bot(f"NineTwenty limit order placed SucessFully !!! \nMessage : {order_number}")
                    self.smart_executer(stratagy=stratagy,sl_percent=sl_percent,option_type=option_type )


            elif not is_order_placed :
                emergency_bot(f'Not able to palce NineTwenty order \nmessage : {order_number}')

        if stratagy in [F.TenThirty, F.Eleven]:
            trigger_price = price+0.05
            
            is_order_placed,order_number  = OrderExecuation(self.broker_name, self.broker_session).place_order(price,trigger_price,qty,ticker,transaction_type,tag)
            if is_order_placed : 
                time.sleep(5)
                all_orders,filled_order,pending_order = order_book()
                all_filled_orders = pending_order[F.order_id].to_list()+filled_order[F.order_id].to_list()
                if order_numberin in all_filled_orders:
                    order = {
                            F.entry_time:str(dt.now()),
                            F.ticker  : ticker,
                            F.token : get_token(ticker),
                            F.transaction_type : transaction_type,
                            F.option_type : option_type,
                            F.qty: qty,
                            #-------------- Entry order details -------------
                            F.entry_orderid : order_number,
                            F.entry_orderid_status  : F.pending_order,    #To check order is complete
                            F.entry_price : price,
                            F.entry_price_initial: price,
                            F.entry_tag : tag,  #tag_contains stratagy_name+option_type+loop no
                            #-------------- sl order details -------------
                            F.sl_orderid :'---',
                            F.sl_orderid_status :'---',
                            F.sl_price:0,
                            F.sl_price_initial:0,
                            F.sl_tag: '',  #tag_contains stratagy_name+option_type+loop no
                            #-------- Other parameter --------------
                            F.exit_price:0,
                            F.exit_time:'---',
                            F.exit_reason : '---',              # sl_hit/day_end
                            F.stratagy : stratagy,
                            F.loop_no:loop_no,
                            F.sl_percent:sl_percent,
                            F.charges : 0,
                            F.drift_points : 0,
                            F.drift_rs : 0,
                            F.recording: [
                                # {'Time':'10:15:00','pl':100},
                                ]
                            }

                    self.entry_id [str(self.date )].insert_one(order)
                    logger_bot(f"NineThirty order placed SucessFully !!! \nMessage : {order_number}")
                    self.smart_executer(stratagy=stratagy,sl_percent=sl_percent,option_type=option_type)
            elif not is_order_placed :
                    emergency_bot(f'Not able to palce NineThirty order \nmessage : {order_number}')

        if stratagy == F.NineFourtyFive:
            price=round(price*0.95,1)
            trigger_price = price+0.05
            
            is_order_placed,order_number  = OrderExecuation(self.broker_name, self.broker_session).place_order(price,trigger_price,qty,ticker,transaction_type,tag)
            if is_order_placed : 
                time.sleep(5)
                all_orders,filled_order,pending_order = order_book()
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
                            F.entry_orderid_status  : F.pending_order,    #To check order is complete
                            F.entry_price : price,
                            F.entry_price_initial: price,
                            F.entry_tag : tag,  #tag_contains stratagy_name+option_type+loop no
                            #-------------- sl order details -------------
                            F.sl_orderid :'',
                            F.sl_orderid_status :'',
                            F.sl_price:0,
                            F.sl_price_initial:0,
                            F.sl_tag: '',  #tag_contains stratagy_name+option_type+loop no
                            #-------- Other parameter --------------
                            F.exit_price:0,
                            F.exit_time:'',
                            F.exit_reason : '',              # sl_hit/day_end
                            F.stratagy : stratagy,
                            F.loop_no:loop_no,
                            F.sl_percent:sl_percent,
                            F.charges : 0,
                            F.drift_points : 0,
                            F.drift_rs : 0,
                            F.recording: [
                                # {'Time':'10:15:00','pl':100},
                                ]
                            }

                    self.entry_id [str(self.date )].insert_one(order)
                    logger_bot(f"{F.NineFourtyFive} order placed SucessFully !!! \nMessage : {order_number}")

            elif not is_order_placed :
                emergency_bot(f'Not able to palce NineThirty order \nmessage :{order_number}')

    def place_limit_sl(self,ticker,qty,transaction_type,avg_price,sl_percent,tag):
        if transaction_type == F.Buy:
            transaction_type =  F.Sell

        elif transaction_type == F.Sell:
            transaction_type =  F.Buy
        else:
            pass


        stoploos = round(round(0.05*round(avg_price/0.05),2)*((100+sl_percent)/100),1)
        trigger_price =round((stoploos)-0.1,2) # 0.05 is the diffrance betweeen limit price and trigger price
        sl_placed,order_number = OrderExecuation(self.broker_name,self.broker_session).place_order(price = stoploos, trigger_price = trigger_price , qty=qty, ticker =ticker , transaction_type = transaction_type, tag = tag+'_sl')
        if sl_placed:
            self.entry_id [str(self.date )].update_one({ "entry_tag": tag}, { "$set": { F.sl_orderid : order_number,  F.sl_orderid_status :  F.open , F.sl_price:stoploos, F.sl_tag: tag+'_sl'} } )
            logger_bot(f"Sl order palced Sucessfully !!! \nMessage : {order_number}\nPrice : {stoploos}")

        elif not sl_placed:
            emergency_bot(f'Problem in palcing limit_sl\nMessage : {order_number}')

    def smart_executer(self,stratagy,sl_percent,option_type) :
        count = 0
        while True:
            order_details =  Order_details(self.broker_session,self.broker_name)
            all_orders,filled_order,pending_order = order_details.order_book()
            myquery = { F.stratagy: { "$eq": stratagy }, F.option_type: { "$eq": option_type } , F.entry_orderid_status: {"$eq":  F.pending_order }}
            db_data = self.entry_id [str(self.date )].find(myquery)
            pending_orders_db = pd.DataFrame(db_data)
            if len(pending_orders_db)!=0:
                print("New loop started\n")

                for index,row in pending_orders_db.iterrows():
                    if (row[ F.entry_orderid]  not in pending_order[F.order_id].tolist()):
                        self.entry_id [str(self.date )].update_one({  F.entry_orderid : row[ F.entry_orderid]}, { "$set": {  F.entry_orderid_status:  F.placed_sucessfully } }) # it will update if sl hit  modificarion status to slhit
                        logger_bot(f'Placed in broker: {row[ F.entry_orderid]}')
                        time.sleep(1)

                    elif count<5:
                        ltp = get_ltp(row[F.token],self.broker_name)
                        # ltp = 40
                        price = row[ F.entry_price ]

                        if ltp>price:
                            new_price  =round(abs(price)-abs(ltp-price)/2,1)
                        else:
                            new_price  =round(abs(price)+abs(ltp-price)/2,1)
                        print(f"ltp :{ltp} old_price:{price} new_price :{new_price} count : {count}\n\n")
                        print("pending_tag :\n ",pending_order[F.order_id])
                        print("F.entry_tag : ",row[F.order_id] )
                        
                        print("pending_orders :\n ",pending_order[pending_order[F.tag]==row[F.entry_tag]])
                        remaning_qty = pending_order[pending_order[F.tag]==row[F.entry_tag]].iloc[0][F.qty]
                        is_modified, order_number = OrderExecuation(self.broker_name,self.broker_session).modify_order(order_id = row[ F.entry_orderid], new_price =new_price ,quantity = remaning_qty)
                        if is_modified :
                            self.entry_id [str(self.date )].update_one({ F.entry_orderid : row[ F.entry_orderid]}, { "$set": {  F.entry_price : new_price } }) # it will update updated price to database
                            logger_bot(f"Entry price modified \nMessage :{order_number} order modified\nOld Price : {price} New Price : {new_price} \nRemaning Qty : {remaning_qty}")
                            count =count + 1
                        elif not is_modified :
                            emergency_bot(f'Not able to modify sl in Smart Execuator(kotak.modify_order)\nMessage : {order_number}')

                    else:
                        remaning_qty = pending_order[pending_order[F.tag]==row[F.entry_tag]].iloc[0][F.qty]
                        is_modified, order_number = OrderExecuation(self.broker_name,self.broker_session).modify_order(order_id = row[ F.entry_orderid], new_price =0 ,quantity = remaning_qty,trigger_price =0, order_type = "MKT")
                        if is_modified :
                            logger_bot(f"Executed at Market Order \nMessage :{order_number} \nRemaning Qty : {remaning_qty}")
                        elif not is_modified :
                            emergency_bot(f'Not able to modify sl in Smart Executer SOD Market Order\nMessage : {order_number}')
                            
                    time.sleep(5)


            else:
                myquery = { F.entry_orderid_status: {"$eq":  F.placed_sucessfully},F.stratagy: { "$eq": stratagy }, F.option_type: { "$eq": option_type },F.sl: { "$eq": '---' }}
                pending_orders_db = self.entry_id [str(self.date )].find(myquery)
                for i in pending_orders_db:
                    tag = f'{i[F.stratagy]}_{i[ F.option_type]}_{i[F.loop_no]}'
                    self.place_limit_sl(ticker=i[ F.ticker ],qty=i[F.qty],transaction_type=i[ F.transaction_type],avg_price=(i[ F.entry_price ]),sl_percent=sl_percent,tag = tag)

    def exit_orders_dayend(self,option_type) :
        # Cancel Pending order
        myquery = {'$or': [{ F.sl_orderid_status :  F.pending_order},{ F.sl_orderid_status : F.reentry_pending_order}]}
        db_data = self.entry_id [str(self.date )].find(myquery)
        for i in db_data:
            try:
                order_number = OrderExecuation(self.broker_name,self.broker_session).cancel_order(i[F.sl_orderid])
                logger_bot(f"pending order \nMessage :{order_number} is canceled")
            except Exception as e:
                emergency_bot(f'Not able to cancle order {i[F.sl_orderid]} in cancel_pending_order()')

        count = 0
        while True:
            myquery = { F.option_type:{'$eq':option_type},'$or': [{ F.sl_orderid_status : F.open},{ F.sl_orderid_status : F.re_entry_open}]}
            db_data = self.entry_id [str(self.date )].find(myquery)
            pending_orders_db = pd.DataFrame(db_data)
            all_orders,filled_order,pending_order = order_book()

            if len(pending_orders_db)!=0:
                for index,row in pending_orders_db.iterrows():
                    if (row[F.sl_orderid]  not in pending_order[F.order_id].tolist()):
                        # print(pending_order[F.order_id].tolist())
                        exit_price = filled_order[filled_order[F.order_id]==row[F.sl_orderid]].iloc[0][F.price]
                        exit_time = dt.now()
                        sl_orderid_status = F.closed
                        exit_reason = 'day_end'
                        self.entry_id [str(self.date )].update_one({ F.sl_orderid : row[F.sl_orderid]}, { "$set": { F.sl_orderid_status : sl_orderid_status, F.exit_reason:exit_reason, F.exit_price:float(exit_price), F.exit_time:str(exit_time)} } )

                    elif count<10:
                        ltp = get_ltp(row[F.token],self.broker_name)
                        remaning_qty = pending_order[pending_order[F.order_id]==row[F.sl_orderid]].iloc[0][F.qty]
                        is_modified, order_number  = OrderExecuation(self.broker_name,self.broker_session).modify_order(order_id = row[F.sl_orderid], new_price = ltp, quantity =remaning_qty)
                        if is_modified :
                            logger_bot(f"Exit price modified \nMessage :{order_number}\nPrice : {ltp}")
                            count =count + 1
                            time.sleep(1)
                        elif not is_modified :
                            emergency_bot(f'Not able to modify sl in exit_orders_dayend(kotak.modify_order)\nMessage : {order_number}')

                    else:
                        remaning_qty = pending_order[pending_order[F.order_id]==row[F.sl_orderid]].iloc[0][F.qty]
                        
                        is_modified, order_number  = OrderExecuation(self.broker_name,self.broker_session).modify_order(order_id = row[F.sl_orderid], new_price = 0, quantity =remaning_qty,trigger_price = 0, order_type = "MKT")
                        if is_modified :
                            logger_bot(f"Executed at Market Order \nMessage :{order_number}")
                        elif not is_modified:
                            emergency_bot(f'Not able to modify sl in exit_orders_dayend() Market Order\nMessage : {order_number}')

            else:
                break


            # else:
            #     def cancel_pending_order():
            #         all_orders,filled_order,pending_order  = order_book()
            #         myquery = { F.option_type:{'$eq':option_type},'$or': [{ F.sl_orderid_status : F.open},{ F.sl_orderid_status : F.re_entry_open}]}
            #         db_data = self.entry_id [str(date)].find(myquery)
            #         for i in :



            #     place_sl(stratagy,sl_percent)
            #     break
