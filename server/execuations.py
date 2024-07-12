from server.utils import kotak_transaction_type_dict,order_staus_dict, Fields as F
from server.broker import is_order_rejected_func
from server.utils import env_variables as env

class OrderExecuation : 
    def __init__(self, broker_name, broker_session):
        self.broker_name = broker_name
        self.broker_session = broker_session
        
    def place_order(self, price, trigger_price, qty, ticker, transaction_type, tag, order_type = 'SL', product_type = None):
        if self.broker_name == F.kotak_neo:
            # Try with Mis Order
            new_tag = tag + '_' + env.product_type
            product_type =  product_type if product_type != None else env.product_type
            # print('product_type : ', product_type)
            responce = self.broker_session.place_order(product = product_type, price = str(price), trigger_price = str(trigger_price), order_type = order_type, quantity = str(qty), trading_symbol = ticker,
                                transaction_type = kotak_transaction_type_dict[transaction_type], amo = "NO", disclosed_quantity = "0", market_protection = "0", pf = "N", validity = "DAY",exchange_segment = "nse_fo",tag = new_tag) #production
            env.logger.info(f'place_order 1 : {responce}\n')
            if responce[F.stCode] == 200 :
                is_order_rejected, is_mis_blocked = is_order_rejected_func(responce[F.nOrdNo], self.broker_session, self.broker_name)
                if is_order_rejected and is_mis_blocked :
                    # Try With NRML order 
                    env.product_type = 'NRML'
                    product_type = 'NRML'
                    new_tag = tag + '_' + product_type
                    responce = self.broker_session.place_order(product = product_type, price = str(price), trigger_price = str(trigger_price), order_type = order_type, quantity = str(qty), trading_symbol = ticker,
                                transaction_type = kotak_transaction_type_dict[transaction_type], amo = "NO", disclosed_quantity = "0", market_protection = "0", pf="N", validity = "DAY",exchange_segment = "nse_fo",tag = new_tag) #production
                    env.logger.info(f'place_order 2 : {responce}\n')
                    if responce[F.stCode] == 200 :
                        is_order_rejected, is_mis_blocked = is_order_rejected_func(responce[F.nOrdNo], self.broker_session, self.broker_name)
                        if not is_order_rejected:
                            # return NRML order
                            return True, responce[F.nOrdNo], product_type, new_tag
                        else :
                            # return MIS order
                            return False, responce, product_type, new_tag
                        
                else :
                    # return MIS order
                    return True, responce[F.nOrdNo], product_type, new_tag
                    
            else :
                return False, responce, product_type , tag
        
    def modify_order(self, order_id, new_price, quantity, trigger_price = None, order_type = "SL"):
        if self.broker_name == F.kotak_neo: 
            responce = self.broker_session.modify_order(order_id = order_id, price = str(new_price), quantity = str(quantity), trigger_price = str(trigger_price if trigger_price != None else new_price-0.05), validity = "DAY", order_type = order_type, amo = "NO")
            env.logger.info(f'modify_order : {responce}\n')
            try : 
                if responce[F.stCode] == 200 :
                    return True, responce[F.nOrdNo], "Placed Sucessfully"
                else : 
                    return False, order_id, str(responce['Error'])
            except Exception as e : 
                return False,order_id , str(responce['Error'])
        
    def cancel_order(self, order_number):
        if self.broker_name == F.kotak_neo: 
            responce = self.broker_session.cancel_order(order_id = order_number)
            env.logger.info(f'cancel_order : {responce}\n')
            try : 
                if responce[F.stCode] == 200 :
                    return True,responce['result'],"Placed Sucessfully"
                else : 
                    return False,0,responce['errMsg']
            except Exception as e : 
                return False,order_id , str(responce)#['Error']
            