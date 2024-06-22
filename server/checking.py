
from server.broker import Order_details,get_ltp,is_order_rejected_func
from server.engine import Checking_Engine
from server.execuations import OrderExecuation
import pandas as pd
from  datetime import datetime as dt
import pymongo
from server.order_management import Order_management
from server.utils import get_db, Fields as F, trailing_points,env_variables as env
from server.utils import emergency_bot,logger_bot

class Checking:
    
    def __init__(self,broker_session,broker_name):
        self.broker_session = broker_session 
        self.broker_name= broker_name 
        self.current_time = dt.now()
        self.date = dt.today().date()
        self.database = get_db()
    
    def check(self):
        is_not_empty = False
        try : 
            order_details =  Order_details(self.broker_session,self.broker_name)
            is_not_empty, all_orders, filled_order, pending_order = order_details.order_book()
            all_positions, open_position, closed_position = order_details.position_book()
        except : 
            pass

        if is_not_empty :
            try:
                self.day_tracker()
                # print(f'\n{self.current_time}---------------- data updated ------------------')
            except Exception as e:
                emergency_bot(f'problem in day_tracker \nReason :{e}')

            try:
                self.fifty_per_management(pending_order,filled_order,F.NineTwenty)
                # print(f'{self.current_time}---------------- fifty_per_management -  {F.NineTwenty} ------------------')
            except Exception as e:
                emergency_bot(f'problem in fifty_per_management for : {F.NineTwenty} \nReason :{e}')

            try:
                self.re_entry_management(pending_order,filled_order,F.NineThirty)
                # print(f'{self.current_time}---------------- re_entry_management -  {F.NineThirty} ------------------')
            except Exception as e:
                emergency_bot(f'problem in re_entry_management for : {F.NineThirty} \nReason :{e}')

            try:
                self.wait_n_trade(pending_order,filled_order,F.NineFourtyFive)
                # print(f'{self.current_time}---------------- wait_n_trade -  {F.NineFourtyFive} ------------------')
            except Exception as e:
                emergency_bot(f'problem in wait_n_trade for : {F.NineFourtyFive} \nReason :{e}')
                
            try:
                self.re_entry_management(pending_order,filled_order,F.TenThirty)
                # print(f'{self.current_time}---------------- re_entry_management -  {F.TenThirty} ------------------')
            except Exception as e:
                emergency_bot(f'problem in re_entry_management for : {F.NineThirty} \nReason :{e}')
            
            try:
                self.re_entry_management(pending_order,filled_order,F.Eleven)
                # print(f'{self.current_time}---------------- re_entry_management -  {F.Eleven} ------------------')
            except Exception as e:
                emergency_bot(f'problem in re_entry_management for : {F.NineThirty} \nReason :{e}')

            try:
                self.check_ltp_above_sl(pending_order,filled_order)
                # print(f'{self.current_time}---------------- check_ltp_above_sl ------------------')
            except Exception as e:
                emergency_bot(f'problem in check_ltp_above_sl\nReason :{e}')
                
            try:
                self.rejected_order_management()
                # print(f'{self.current_time}---------------- rejected_order_management ------------------')
            except Exception as e:
                emergency_bot(f'problem in rejected_order_management\nReason :{e}')
                
                
        else :
            pass
        # try:
        #     self.is_loss_above_limit(pl)
        # except Exception as e:
        #     print('problem in check_ltp_above_sl',e)
        
    def day_tracker(self):
        # -------------------------------- Upload each leg wise pl to order db ---------------------------------------------
        if env.day_tracker : 
            myquery = {'$or': [{F.exit_orderid_status : F.open},{F.exit_orderid_status : F.re_entry_open}]}
            db_data = self.database[str(self.date)].find(myquery)
            # print(pd.DataFrame(db_data))
            for i in db_data:
                ltp = get_ltp(i[F.token], self.broker_name)
                pl =  round((i[F.entry_price] - ltp) * i[F.qty])
                # print('order_id: ',i[F.exit_orderid] )
                self.database[str(self.date)].update_one({F.exit_orderid : i[F.exit_orderid]},{'$push' : {F.recording: {'Time': self.current_time, 'pl' : pl}}}) #procuction
            
                # entry_id[str(date)].update_one({F.ticker :i[F.ticker]},{'$push': {F.recording: {'Time':current_time , 'pl': pl,'datetime':dt.now()}}})


        # # -------------------------------- Upload Stratagy wise pl to pl db ---------------------------------------------
    
    def Update_commmon_pl(self):

        db_data = self.database[str(self.date)].find()
        # stratagy=F.NineFourtyFive
        NineTwenty = []
        NineThirty = []
        NineFourtyFive = []
        for i in db_data:
            if i[F.stratagyy] == F.NineTwenty:
                for j in (i[F.recording]):
                    NineTwenty.append(j)
                reecord_pl[str(self.date)].update_one({F.stratagyy : {'$eq"':  F.NineTwenty}}, {F.recording : NineTwenty })

            elif i[F.stratagy] == F.NineThirty:
                for j in (i[F.recording]):
                    NineThirty.append(j)
                reecord_pl[str(self.date)].update_one({F.stratagy : {'$eq"':  F.NineThirty}}, {F.recording : NineThirty })

            elif i[F.stratagy] == F.NineFourtyFive:
                for j in (i[F.recording]):
                    NineFourtyFive.append(j)
                reecord_pl[str(self.date)].update_one({F.stratagy : {'$eq"':  F.NineFourtyFive}}, {F.recording : NineFourtyFive })

        # print( F.NineTwenty , NineTwenty)
        combine_pl = NineTwenty + NineThirty + NineFourtyFive

        a = pd.DataFrame(combine_pl).groupby('Time').sum().reset_index()
        reecord_pl[str(self.date)].update_one({F.stratagy : {'$eq"': 'combine_pl'}},{F.recording : combine_pl })

    def fifty_per_management(self,pending_order,filled_order,stratagy):
        NineTwenty_db = self.database[str(self.date)].find({F.stratagy : {'$eq':stratagy}})
        pending_order_list = pending_order[F.order_id].to_list()
        for i in NineTwenty_db:
            if i[F.exit_orderid] not in pending_order_list and (i[F.exit_orderid_status] ==  F.open) :
                sl_price = filled_order[filled_order[F.order_id] == i[F.exit_orderid]].iloc[0][F.price]
                exit_time = self.current_time
                self.database[str(self.date)].update_one({F.entry_orderid : i[F.entry_orderid]}, { "$set": {F.exit_orderid_status : F.closed, F.exit_reason : F.sl_hit, F.exit_order_execuation_type : F.limit_order, F.exit_price : float(sl_price), F.exit_time : str(exit_time)}} )
                logger_bot(f'Sl hit {i[F.exit_orderid]}\nStatagy : {i[F.stratagy]}\nSide : {i[F.option_type]}')
            else:
                pass
        
    def re_entry_management(self,pending_order,filled_order,stratagy):
        NineThirty_db = self.database[str(self.date)].find({F.stratagy : {'$eq': stratagy}})
        pending_order_list = pending_order[F.order_id].to_list()
        for i in NineThirty_db:
            count = i[F.exit_order_count]
            if (i[F.exit_orderid] not in pending_order_list) and (i[F.exit_orderid_status] ==  F.open) and (i[F.exit_reason] == '---'): # re-entry pending order place
                #----------------------------------- Upadate 1st order details ---------------------------------------------------------
                sl_price = filled_order[filled_order[F.order_id] == i[F.exit_orderid]].iloc[0][F.price]
                self.database[str(self.date)].update_one({F.exit_orderid : i[F.exit_orderid]}, { "$set": {F.exit_orderid_status : F.closed, F.exit_reason : F.sl_hit, F.exit_price : float(sl_price), F.exit_order_execuation_type : F.limit_order, F.exit_time : str(self.current_time), F.exit_order_count : count + 1 } } )
                logger_bot(f'Sl hit {i[F.exit_orderid]}\nTicker : {i[F.ticker]}\nStatagy : {i[F.stratagy]}\nSide : {i[F.option_type]}')
                #----------------------------------- Place re-entry order ---------------------------------------------------------
                tag = f'{i[F.stratagy]}_{i[F.option_type]}_{i[F.loop_no]}_re_entry'
                trigger_price = i[F.entry_price] + 0.05
                # print('trigger_price : ',trigger_price)
                # print(f"price = {i[F.entry_price]},trigger_price = {trigger_price},qty = {i[F.qty]},ticker ={ i[F.ticker]},transaction_type = {[F.transaction_type]},tag = tag")
                is_order_placed, order_number, product_type, tag = OrderExecuation(self.broker_name, self.broker_session).place_order(price = i[F.entry_price], trigger_price = trigger_price, qty = i[F.qty], ticker = i[F.ticker], transaction_type = i[F.transaction_type], tag = tag)
                if is_order_placed : 
                    order = { 
                            F.entry_time : '---',
                            F.ticker : i[F.ticker],
                            F.token : i[F.token],
                            F.transaction_type : i[F.transaction_type],
                            F.option_type : i[F.option_type],
                            F.qty : i[F.qty],
                            #-------------- Entry order details -------------
                            F.entry_orderid : order_number,
                            F.entry_orderid_status : F.re_entry_open,    #To check order is complete
                            F.entry_price : i[F.entry_price],
                            F.entry_price_initial : i[F.entry_price],
                            F.entry_order_count : 0,
                            F.entry_order_execuation_type : None,
                            F.entry_tag : tag,
                            #-------------- sl order details -------------
                            F.exit_orderid :'---',
                            F.exit_orderid_status :'---',
                            F.exit_price : 0,
                            F.exit_price_initial : 0,
                            F.exit_tag : '---',  #tag_contains stratagy_name+option_type+loop no
                            #-------- Other parameter --------------
                            F.exit_price : 0,
                            F.exit_time : '---',
                            F.exit_reason : '---',              # sl_hit/day_end
                            F.exit_order_count : 0,
                            F.exit_order_execuation_type : None,
                            F.stratagy : i[F.stratagy],
                            F.index : env.index,
                            F.loop_no : i[F.loop_no],
                            F.exit_percent : i[F.exit_percent],
                            F.charges : 0,
                            F.drift_points : 0,
                            F.drift_rs : 0,
                            F.recording : [
                                # {'Time':'10:15:00','pl':100},
                           ]
                            }

                    self.database[str(self.date)].insert_one(order)
                    logger_bot(f"re-entry order palced Sucessfully !!! \nMessage : {order_number}\nTicker : {i[F.ticker]}\nPrice : {i[F.entry_price]}\nStratagy : {i[F.stratagy]}\nSide : {i[F.option_type]}")
                else : 
                    
                    pass# handel issue in emergrncy 

            if (i[F.entry_orderid] not in pending_order_list) and (i[F.entry_orderid_status] == F.re_entry_open) : 
                # Check is re-entry pending order is threre if not place sl for re-entry 
                self.database[str(self.date)].update_one({F.entry_orderid : i[F.entry_orderid]}, { "$set": {F.entry_orderid_status : F.closed, F.entry_time : self.current_time, F.entry_order_execuation_type : F.limit_order, F.entry_order_count : count + 1 } } )

                #------------- Place new re-entry stoploss --------------

                if i[F.transaction_type] == F.Buy:
                    transaction_type =  F.Sell

                elif i[F.transaction_type] == F.Sell:
                    transaction_type =  F.Buy

                tag = i[F.entry_tag]
                ticker = i[F.ticker]
                stoploos = round(i[F.entry_price] + (i[F.entry_price] * (i[F.exit_percent] / 100)),1)
                qty = i[F.qty]
                trigger_price = stoploos - 0.1
                is_order_placed, order_number, product_type, new_tag = OrderExecuation(self.broker_name,self.broker_session).place_order(price = stoploos, trigger_price = trigger_price, qty = qty, ticker = ticker , transaction_type = transaction_type, tag = tag + '_sl')
                if is_order_placed : 
                    self.database[str(self.date)].update_one({F.entry_orderid : i[F.entry_orderid]}, { "$set": {F.exit_orderid : order_number,  F.exit_orderid_status : F.re_entry_open, F.entry_order_execuation_type : F.limit_order, F.exit_price : stoploos, F.exit_price_initial : stoploos , F.exit_tag : tag + '_sl'} } )
                    logger_bot(f"re-entry Sl order palced Sucessfully !!! \nMessage : {order_number}\nTicker : {i[F.ticker]}\nPrice : {stoploos}\nStratagy : {i[F.stratagy]}\nSide : {i[F.option_type]}")
                elif not is_order_placed:
                    self.database [str(self.date)].update_one({"entry_tag": tag}, {"$set": {F.exit_orderid_status : F.rejected }})
                    emergency_bot(f'Problem in palcing limit_sl\nStratagy : {i[F.entry_orderid]}\Side : {i[F.option_type]}\nMessage : {message}')
                
            if (i[F.exit_orderid] not in pending_order_list) and i[F.exit_orderid_status] == F.re_entry_open :
                # Track re-entry sl order is sl hit
                sl_price = filled_order[filled_order[F.order_id] == i[F.exit_orderid]].iloc[0][F.price]
                self.database[str(self.date)].update_one({F.exit_orderid : i[F.exit_orderid]}, { "$set": {F.exit_orderid_status : F.closed, F.exit_reason : F.sl_hit, F.exit_order_execuation_type : F.limit_order, F.exit_price : float(sl_price), F.exit_time : str(self.current_time), F.exit_order_count : count + 1} } )
                logger_bot(f"re-entry Sl hit !!! \nMessage : {i[F.exit_orderid]}\nTicker : {i[F.ticker]}\nPrice : {sl_price}\nStratagy : {i[F.stratagy]}\nSide : {i[F.option_type]}")
                         
    def wait_n_trade(self,pending_order,filled_order,stratagy):
        query = {F.stratagy : {'$eq': stratagy}}
        NineFourtyFive_db = self.database[str(self.date)].find(query)
        pending_order_list = pending_order[F.order_id].to_list()

        for i in NineFourtyFive_db:
            count = i[F.exit_order_count]
            if (i[F.entry_orderid_status] == F.open) and (i[F.entry_orderid] not in pending_order_list) and (i[F.exit_reason] == '---'):
                logger_bot(f'Placed at broker-end  :  {i[F.exit_orderid]}\nTicker : {i[F.ticker]}\nStatagy : {i[F.stratagy]}\nSide : {i[F.option_type]}')
                #--------------- Place limit sl ----------------------------
                tag = f"{i[F.stratagy]}_{i[F.option_type]}_{i[F.loop_no]}"
                Order_management(self.broker_name, self.broker_session).place_limit_sl(i[F.ticker], i[F.qty], i[F.transaction_type], i[F.entry_price], i[F.exit_percent], i[F.option_type], tag)
                ltp = get_ltp(i[F.token],self.broker_name)
                self.database[str(self.date)].update_one({F.entry_orderid : {'$eq':i[F.entry_orderid]}},{"$set" : {F.entry_orderid_status : F.closed ,F.entry_time : self.current_time, F.entry_order_execuation_type : F.limit_order, "ltp" : ltp, F.entry_order_count : count + 1}})
            if (i[F.exit_orderid_status] == F.open):
                #--------------- Trail sl ----------------------------
                new_ltp = get_ltp(i[F.token],self.broker_name)
                points =  trailing_points()
                net_points = (i["ltp"] - new_ltp)//points
                # print(net_points)
                if net_points > 0:
                    new_sl = i[F.exit_price] -  net_points
                    is_modified,order_number,message = OrderExecuation(self.broker_name,self.broker_session).modify_order(order_id = i[F.exit_orderid], new_price = new_sl, quantity = i[F.qty])
                    is_order_rejected = is_order_rejected_func(order_number, self.broker_session, self.broker_name)
                    if is_modified and not is_order_rejected :  
                        self.database[str(self.date)].update_one({F.entry_orderid : {'$eq' : i[F.entry_orderid]}},{"$set" : {F.exit_price : new_sl, F.exit_price_initial : new_sl, "ltp" : new_ltp}})
                        logger_bot(f"SL trailed from {i[F.exit_price]} to {new_sl} of {i[F.exit_orderid]}\nTicker : {i[F.ticker]}\nSide : {i[F.option_type]}\nStratagy : {i[F.stratagy]}")

            if (i[F.exit_orderid_status]== F.open) and (i[F.exit_orderid] not in pending_order_list):
                #--------------- Update sl hit ----------------------------
                sl_price = filled_order[filled_order[F.order_id] == i[F.exit_orderid]].iloc[0][F.price]
                self.database[str(self.date)].update_one({F.entry_orderid :{'$eq' : i[F.entry_orderid]}},{"$set" : {F.exit_orderid_status : F.closed, F.exit_price : sl_price, F.exit_reason : F.sl_hit, F.exit_order_execuation_type : F.limit_order, F.exit_order_count : count + 1 , F.exit_time : self.current_time}})
                logger_bot(f"Sl hit !!! \nMessage : {i[F.exit_orderid]}\nTicker : {i[F.ticker]}\nPrice : {sl_price}\nStratagy : {i[F.stratagy]}\nSide : {i[F.option_type]}")
        
    def check_ltp_above_sl(self,pending_order,filled_order):
        myquery = {'$or': [{F.exit_orderid_status : F.open},{F.exit_orderid_status : F.re_entry_open}]}
        db_data = self.database[str(self.date)].find(myquery)
        
        for i in db_data:
            ltp = get_ltp(i[F.token],self.broker_name)
            sl_price = i[F.exit_price]
            if ltp > sl_price:
                new_price = round(abs(sl_price) + abs(ltp - sl_price)/2 ,1)
                remaning_qty = i[F.qty]
                count = i[F.exit_order_count]
                qty = pending_order[pending_order[F.order_id] == i[F.exit_orderid]].iloc[0][F.qty]
                # print(f'order_id = {i[F.exit_orderid]},new_price={new_price},quantity = {qty}')
                is_modified, order_number,message = OrderExecuation(self.broker_name, self.broker_session).modify_order(order_id = i[F.exit_orderid], new_price = new_price, quantity = qty)
                is_order_rejected = is_order_rejected_func(order_number,self.broker_session,self.broker_name)
                if is_modified and not is_order_rejected :  
                    logger_bot(f"ltp is above stoploss  \nMessage :{order_number} order modified\nTicker: {i[F.ticker]}\nPrice : {new_price}")
                    self.database[str(self.date)].update_one({F.exit_orderid : i[F.exit_orderid]}, { "$set": {F.exit_price : new_price, F.exit_order_count : count + 1 } }) # it will update updated price to database
                else : 
                    emergency_bot(f'Not able to modify sl in check_ltp_above_sl\nMessage : {order_number}') 
                    


            else :
                # print('-----------',ltp,sl_price,i[F.exit_orderid])
                pass
    
    def rejected_order_management(self):
        myquery = {'$or': [{F.exit_orderid_status : F.rejected},{F.entry_orderid_status : F.rejected}]}
        db_data = self.database[str(self.date)].find(myquery)
        for i in db_data : 
            emergency_bot(f'Rejected order... \nStratagy : {i[F.stratagy]}\nExit order id : {i[F.exit_orderid]}\nOption Type : {i[F.option_type]}')
            

    # def is_loss_above_limit(self,pl):
    #     if  pl<(-acoount_balance*(2/100)):
    #         emergency_bot(f'Loss is above the limit Please Check it \nP/l : {pl}')

