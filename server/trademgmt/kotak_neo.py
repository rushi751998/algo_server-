import neo_api_client
import logging
from ..models.OrderStatus import OrderStatus

class KotakManager:
    def __init__(self):
        pass
    # self.broker = broker
    # self.brokerHandle = None

    def placeOrder(self, orderInputParams):
        product_type =  product_type if product_type != None else env.product_type
        # print('product_type : ', product_type)
        responce = self.broker_session.place_order(product = product_type, 
                                                price = str(price), 
                                                trigger_price = str(trigger_price), 
                                                order_type = order_type, 
                                                quantity = str(qty), 
                                                trading_symbol = ticker,
                                                transaction_type = self.convertToBrokerDirection(transaction_type),
                                                amo = "NO", 
                                                disclosed_quantity = "0", 
                                                market_protection = "0", 
                                                pf = "N", 
                                                validity = "DAY",
                                                exchange_segment = "nse_fo",
                                                tag = new_tag) #production
        env.logger.info(f'place_order MIS : {responce}\n')
        if responce[F.stCode] == 200 :
            is_mis_order_rejected, is_mis_blocked = is_order_rejected_func(responce[F.nOrdNo], self.broker_session, self.broker_name)
            if is_mis_order_rejected and is_mis_blocked :
                # Try With NRML order 
                env.product_type = 'NRML'
                product_type = 'NRML'
                new_tag = tag + '_' + product_type
                responce = self.broker_session.place_order(product = product_type, 
                                                        price = str(price), 
                                                        trigger_price = str(trigger_price), 
                                                        order_type = self.convertToBrokerOrderType(order_type), 
                                                        quantity = str(qty), 
                                                        trading_symbol = ticker,
                                                        transaction_type = self.convertToBrokerDirection(transaction_type), 
                                                        amo = "NO", 
                                                        disclosed_quantity = "0", 
                                                        market_protection = "0", 
                                                        pf="N", 
                                                        validity = "DAY",
                                                        exchange_segment = "nse_fo",
                                                        tag = new_tag) #production
                env.logger.info(f'place_order 2 : {responce}\n')
                if responce[F.stCode] == 200 :
                    is_nrml_order_rejected, _ = is_order_rejected_func(responce[F.nOrdNo], self.broker_session, self.broker_name)
                    if not is_nrml_order_rejected:
                        # return NRML order accepted
                        return True, responce[F.nOrdNo], product_type, new_tag
                    else :
                        # return NRML order rejected
                        return False, responce, product_type, new_tag
                    
            elif not is_mis_order_rejected:
                # return MIS order accepted
                return True, responce[F.nOrdNo], product_type, new_tag
                
            else :
                # return MIS order rejected
                return False, responce, product_type, new_tag

    def modifyOrder(self, orderModifyParams):
        responce = broker_session.modify_order(order_id = order_id, 
                                            price = str(new_price), 
                                            quantity = str(quantity), 
                                            trigger_price = str(trigger_price if trigger_price != None else new_price-0.05), 
                                            validity = "DAY", order_type = order_type, amo = "NO")
        logging.info(f'modify_order : {responce}\n')
        try : 
            if responce[F.stCode] == 200 :
                return True, responce[F.nOrdNo], "Placed Sucessfully"
            else : 
                return False, order_id, str(responce['Error'])
        except Exception as e : 
            return False,order_id , str(responce['Error'])

    def modifyOrderToMarket(self, order):
        responce = broker_session.modify_order(order_id = order['order_id'], 
                                            new_price = 0, 
                                            quantity = order['qty'], 
                                            trigger_price = 0, 
                                            order_type = "MKT")

    def cancelOrder(self, order):
        responce = self.broker_session.cancel_order(order_id = order_number)
        logging.info(f'cancel_order : {responce}\n')
        try : 
            if responce[F.stCode] == 200 :
                return True,responce['result'],"Placed Sucessfully"
            else : 
                return False,0,responce['errMsg']
        except Exception as e : 
            return False,order_id , str(responce)#['Error']

    def is_order_rejected_func(order_id,broker_session):
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

    def fetchAndUpdateAllOrderDetails(df):
        try :
            responce = broker_session.order_report()
            if responce[F.stCode] == 200 : 
                all_orders = pd.DataFrame(responce[F.data])[[F.nOrdNo,'ordDtTm','trdSym','tok',F.qty,'fldQty','avgPrc','trnsTp','prod' ,'exSeg','ordSt','stkPrc','optTp','brdLtQty','expDt','GuiOrdId','rejRsn']]
                all_orders = KotakManager.rename_columns(all_orders)
                all_orders = all_orders[all_orders['exchange_segement']=='nse_fo']
                all_orders.iloc[all_orders['order_status']=='complete','order_status'] = OrderStatus.COMPLETE
                all_orders.iloc[all_orders['order_status']=='trigger pending','order_status'] = OrderStatus.TRIGGER_PENDING
                all_orders.iloc[all_orders['order_status']=='open','order_status'] = OrderStatus.OPEN
                all_orders.iloc[all_orders['order_status']=='complete','order_status'] = OrderStatus.COMPLETE
                # pending_order = all_orders[all_orders['order_status'] == 'open']
                # pending_order = all_orders[all_orders['order_status'].isin(['trigger pending','open'])]
                
        except Exception as e:
            send_message(message = f'Not able to get orderbook\nMessage : {e}\nresponce : {responce}', emergency = True)
            return False,all_orders,filled_order,pending_order

    
    @staticmethod
    def convertToBrokerProductType(productType):
        if productType == ProductType.MIS:
            return "MIS"
        elif productType == ProductType.NRML:
            return "NRML"
        elif productType == ProductType.CNC:
            return "CNC"
        return None 

    @staticmethod
    def convertToBrokerOrderType(orderType):
        kite = self.brokerHandle
        if orderType == OrderType.LIMIT:
            return "L"
        elif orderType == OrderType.MARKET:
            return "MKT"
        elif orderType == OrderType.SL_MARKET:
            return "SL-M"
        elif orderType == OrderType.SL_LIMIT:
            return "SL"
        return None

    @staticmethod
    def convertToBrokerDirection(direction):
        if direction == Direction.LONG:
            return 'Buy'

        elif direction == Direction.SHORT:
            return 'Sell'
        return None
    
    @staticmethod
    def rename_columns(df):
        column_name_dict = {
            # Order Related
            'nOrdNo': F.order_id,
            'ordDtTm': F.order_time,
            'trdSym': F.ticker,
            'tok': F.token,
            'qty': F.qty,
            'fldQty': F.filled_qty,
            'avgPrc': F.price,
            'trnsTp': F.transaction_type,
            'prod' : F.product_type,
            'exSeg': 'exchange_segement',
            'ordSt': F.order_status,
            'stkPrc': F.strike_price,
            'strike': F.strike_price,
            'optTp': F.option_type,
            'brdLtQty':'brdLtQty',
            'expDt': F.expiry_date,
            'GuiOrdId': F.tag,
            'type' : 'type',
            'buyAmt' : F.buy_amount,
            'flBuyQty' : F.filed_buy_qty,
            'flSellQty' : F.filed_sell_qty,
            'sellAmt' : F.sell_amount,
            'rejRsn' : F.message,
            
            # Script Master
            'pSymbol' : F.token ,
            'pTrdSymbol' : F.ticker ,
            'pSymbolName' : F.index ,
            'lExpiryDate' : F.expiry_date ,
            'dStrikePrice;' : F.strike_price ,
            'pOptionType' : F.option_type ,
            'lLotSize' : F.lot_size ,
        }
        return df.rename(columns = column_name_dict)
        
    



