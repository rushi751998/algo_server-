from utils import kotak_transaction_type_dict,order_staus_dict, Fields as F

class OrderExecuation : 
    def __init__(self,broker_name,broker_session):
        self.broker_name = broker_name
        self.broker_session = broker_session
        
    def place_order(self,price,trigger_price,qty,ticker,transaction_type,tag,order_type = 'SL'):
        if self.broker_name  == F.kotak_neo:
            
            responce = self.broker_session.place_order(product="MIS", price=str(price), trigger_price=str(trigger_price), order_type=order_type, quantity=str(qty), trading_symbol=ticker,
                                transaction_type= kotak_transaction_type_dict[transaction_type], amo="NO", disclosed_quantity="0", market_protection="0", pf="N", validity="DAY",exchange_segment="nse_fo",tag=tag) #production

            if responce[F.stCode] == 200 :
                return True,responce[F.nOrdNo]
            else : 
                return False,responce['errMsg']
        
    def modify_order(self,order_id,new_price,quantity,trigger_price = None,order_type = "SL"):
        if self.broker_name  == F.kotak_neo: 
            responce =self.broker_session.modify_order(order_id = order_id, price = str(new_price), quantity = str(quantity), trigger_price =  str(trigger_price if trigger_price != None else new_price-0.05), validity = "DAY", order_type = order_type, amo = "")
            # print(f'modify_order  : {responce}')
            if responce[F.stCode] == 200 :
                return True,responce[F.nOrdNo]
            else : 
                return False,responce['errMsg']
        
    def cancel_order(self,order_number):
        if self.broker_name  == F.kotak_neo: 
            responce = self.broker_session.cancel_order(order_id = order_number)
            if responce[F.stCode] == 200 :
                return True,responce[F.nOrdNo]
            else : 
                False,responce['errMsg']
                return order_number_number
        