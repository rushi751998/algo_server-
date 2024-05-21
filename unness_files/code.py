# #@title
# import ks_api_client
# import time
# import pandas as pd
# import numpy as np
# from datetime import datetime as dt
# import requests
# from datetime import timedelta
# import datetime
# import pymongo
# from multiprocessing import Process


# client = pymongo.MongoClient('mongodb+srv://rushi:admin@cluster0.snaakz3.mongodb.net/?retryWrites=true&w=majority')
# todays_tickers= client['todays_tickers'] # Database Name
# pd.options.mode.chained_assignment = None # it is mandatory

# date = dt.today().date()
# entry_id= client['order_adjuster']
# tickers_df = client['tickers_df']
# token = client['token']
# ticker ='NSE:NIFTYBANK-INDEX' # ist is used for getinf LTP
# acoount_balance = 400000
# allowed_loss_percent = 1  #lt will ignore loss below the limit if goes above it will reminder


# start_time = datetime.time(9, 15)
# end_time = datetime.time(15,18)
# # first_order = '09:20'
# # second_order = '09:30'
# first_order = '09:21'
# second_order = '09:31'
# exit_order = '15:01'

# def get_ist_now():
#     return dt.now() + timedelta(hours=5.5)

# def emergency_bot(bot_message):
#     """ It is used for sending alert to Emergeny situation"""
#     current_time = get_ist_now()
#     bot_message = f'{dt.strftime(current_time,"%H:%M:%S")}\n{bot_message}'
#     bot_token ='5122704517:AAHbcnci6KKkBR7taNaXwEYbXYMB7lU9lbE'
#     bot_chatId = '644452386'
#     send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatId + '&parse_mode=Markdown&text=' + bot_message
#     response = requests.get(send_text)

# def alert_bot(bot_message):
#     """ It is used for sending price alert"""
#     current_time = get_ist_now()
#     bot_message = f'{dt.strftime(current_time,"%H:%M:%S")}\n{bot_message}'
#     bot_token ='5129893039:AAHp8vD1oo3IDhI3CQCtYvWzF72mK3F8I5Q'
#     bot_chatId = '644452386'
#     send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatId + '&parse_mode=Markdown&text=' + bot_message
#     response = requests.get(send_text)

# def logger_bot(bot_message):
#     """ It is used for sending order manangemant"""
#     current_time = get_ist_now()
#     bot_message = f'{dt.strftime(current_time,"%H:%M:%S")} {bot_message}'
#     bot_token ='5195435501:AAHOn3mTR2GBdrcukx4qwCycqC2G3SY2UqU'
#     bot_chatId = '644452386'
#     send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatId + '&parse_mode=Markdown&text=' + bot_message
#     response = requests.get(send_text)
#     # return response.json()

# def login():
#     key = client['key']
#     access_token = 'f9fdc746-2afa-368b-8a4c-028af9b801c3'
#     consumer_key = 'Tyg4L8AvTThw8OQtoORXeBJnLxEa'
#     secreate_key = 'lzhvHb61csRrc8tlVEEMGMUUMrIa'
#     user_id = 'RU1049'
#     url = 'https://tradeapi.kotaksecurities.com/apim/scripmaster/1.1/filename'
#     password='Rushi@1998'
#     from ks_api_client import ks_api
#     try:
#         kotak = ks_api.KSTradeApi(access_token = access_token, userid = user_id,
#                         consumer_key = consumer_key, ip = "127.0.0.1", app_id = secreate_key)
#         kotak.login(password = password)
#         a =  key[str(dt.today().date())].find()
#         login_key = a[0]['key']
#         check_login = (kotak.session_2fa(access_code = str(login_key))['userId'])
#         if check_login==user_id:
#             logger_bot("Login Sucesfully (EC2)")
#             return kotak
#     except Exception as e:
#         emergency_bot(f'Problem in login Process \nMessage : {e}')

# kotak = login()

# def get_ltp(token,check_oi = False):
#     ticker = kotak.quote(instrument_token=str(token))['success'][0]
#     if check_oi == True:
#         if float(ticker['OI']) >100000:
#             ltp = float(ticker['ltp'])
#             return ltp
#         else:
#             emergency_bot(f'less open intrest : {token}')
#     else:
#         # ltp = float(ticker['ltp'])
#         ltp = float(ticker['ltp'])
#         return ltp

# def order_book():
#     all_orders = pd.DataFrame(kotak.order_report()['success'])[['orderTimestamp','orderId','instrumentToken','instrumentName','price','orderQuantity','transactionType','status','statusMessage','triggerPrice']]
#     pending_order = all_orders[all_orders['status'].isin(['SLO','OPN'])]
#     Filled_orders = all_orders[all_orders['status']=='TRAD']
#     # pd.to_datetime(all_orders['orderTimestamp'],format='%b %d %Y %I:%M:%S:f%p')
#     return pending_order,Filled_orders

# def positions():
#     positions = pd.DataFrame(kotak.positions(position_type = "OPEN")['Success'])[['instrumentName','instrumentToken','averageStockPrice','lastPrice','netTrdQtyLot','deliveryStatus','realizedPL','segment']]
#     all_positions = positions[(positions['instrumentToken']!=0)&(positions['segment']!='EQ')]
#     open_positions = all_positions[all_positions['netTrdQtyLot']!=0]
#     all_positions['pl'] =(all_positions['averageStockPrice']-all_positions['lastPrice'])*(-all_positions['netTrdQtyLot'])
#     pl = round(all_positions['pl'].sum()+positions['realizedPL'].sum(),2)
#     return all_positions,open_positions,pl
# # ------------   Placing   ------------------------

# def get_todays_ticker(check_price,buy_hedges = False):
#     def get_atm(ltp):
#         base = 100
#         atm = base*round(ltp/base)
#         return atm

#     def get_tickers(check_price,atm):
#         volume_check = 1500000

#         ce_strikes = df[(df['optionType']=='CE')&(df['strike']>=atm-400)&(df['strike']<=atm+4000)].sort_values('strike')['instrumentToken'].to_list()
#         pe_strikes = df[(df['optionType']=='PE')&(df['strike']<=atm+400)&(df['strike']>=atm-4000)].sort_values('strike',ascending=False)['instrumentToken'].to_list()

#         ls = []
#         strike = None
#         for i in ce_strikes:
#             lp = get_ltp(i)
#             strike = i
#             if lp<=check_price:
#                 break

#         ls.append(strike)

#         for i in pe_strikes:
#             lp = get_ltp(i)
#             strike = i
#             if lp<=check_price:
#                 break
#         ls.append(strike)

#         return(ls)

#     def hedges_tickers(atm):
#         oi_check = 100000
#         ce_strikes = df[(df['optionType']=='CE')&(df['strike']>=atm)&(df['strike']<=atm+4000)].sort_values('strike',ascending=False)['instrumentToken'].to_list()
#         pe_strikes = df[(df['optionType']=='PE')&(df['strike']<=atm)&(df['strike']>=atm-4000)].sort_values('strike')['instrumentToken'].to_list()
#         ls = []
#         for i in ce_strikes:
#             ticker = kotak.quote(instrument_token=str(i))['success'][0]
#             if float(ticker['OI'])>oi_check:
#                 ls.append(i)
#                 break

#         for i in pe_strikes:
#             ticker = kotak.quote(instrument_token=str(i))['success'][0]
#             if float(ticker['OI'])>oi_check:
#                 ls.append(i)
#                 break

#         return(ls)

#     df = pd.DataFrame(tickers_df['tickers'].find())
#     fut = df[(df['optionType']=='XX')]
#     future_token = fut[fut['days_to_expire']==fut['days_to_expire'].min()].iloc[0]['instrumentToken']
#     days_to_expiry = df[df['optionType']!='XX'].iloc[0]['days_to_expire']
#     atm = get_atm(get_ltp(future_token))


#     if (buy_hedges==True):
#         # qty_to_trade =1*qty
#         buy_hedges =True #here find hedges strikes
#         hedges = hedges_tickers(atm)
#     else :
#         (days_to_expiry !=0)
#         # qty_to_trade = 1*qty
#         buy_hedges =False
#         hedges=[]
#     tickers = get_tickers(check_price,atm)

#     return tickers,buy_hedges,hedges

# def order_place(token,qty,side,stratagy,sl_percent):
#     """It will place order using broker terminal"""
#     if stratagy =='hedges':
#         try :
#             responce = kotak.place_order(order_type = "MIS", instrument_token = token, transaction_type = side,
#                             quantity = qty, price = 0, disclosed_quantity = 0, trigger_price = 0,
#                             tag = "string", validity = "GFD", variety = "REGULAR")
#             # print(f"hedges added SucessFully !!! \nmessage : {responce['Success']['NSE']['message']}")
#             alert_bot(f"hedges added SucessFully !!! \nMessage : {responce['Success']['NSE']['message']}")

#         except Exception as e:
#             emergency_bot(f'Not able to palce hedge order \nMessage : {e}')

#     else:
#         try :
#             ltp = get_ltp(token,True)
#             trigger_price = ltp+0.05
#             responce = kotak.place_order(order_type = "MIS", instrument_token = token, transaction_type = side,
#                             quantity = qty, price = ltp, disclosed_quantity = 0, trigger_price = trigger_price,
#                             tag = "string", validity = "GFD", variety = "REGULAR")
#             entry_id= client['order_adjuster'] # Database Name
#             entry_id[stratagy].insert_one({'order_id':responce['Success']['NSE']['orderId'],'price':ltp,'sl_percent':sl_percent,'token':token,'qty':qty,'status':'pending'})
#             alert_bot(f"Pending Order Placed !!! \nMessage : {responce['Success']['NSE']['message']} \nPrice : {ltp}")
#             return (f"Order Placed SucessFully !!! \nMessage : {responce['Success']['NSE']['message']} \nPrice : {ltp}")


#         except Exception as e:
#             print(e)
#             emergency_bot(f'Not able to palce order \nMessage: {e}')

# def place_limit_sl(token,qty,side,avg_price,sl_percent):
#     """For putting sl use this function in loop
#     it will place sl order with percentage
#     input : Side ,Average Price, Stop_Percentage
#     """
#     if side =='BUY':
#         side = 'SELL'
#         stoploos = round(round(0.05*round(avg_price/0.05),2)*((100+sl_percent)/100),1)
#         trigger_price =round((stoploos)-0.05,2) # 0.05 is the diffrance betweeen limit price and trigger price


#     elif side =='SELL':
#         side = 'BUY'
#         stoploos = round(round(0.05*round(avg_price/0.05),2)*((100+sl_percent)/100),1)
#         trigger_price =round((stoploos)-0.05,2) # 0.05 is the diffrance betweeen limit price and trigger price
#     else:
#         pass


#     try :
#         responce = kotak.place_order(order_type = "MIS", instrument_token = token, transaction_type = side,
#                         quantity = qty, price = stoploos, disclosed_quantity = 0, trigger_price = trigger_price,
#                         tag = "string", validity = "GFD", variety = "REGULAR")
#         alert_bot(f"Sl order palced Sucessfully !!! \nMessage : {responce['Success']['NSE']['message']}\nPrice : {stoploos}")

#     except Exception as e:
#         # print(f'Problem in palcing limit_sl\nError : {e}')
#         emergency_bot(f'Problem in palcing limit_sl\nMessage : {e}')

# def smart_executer(plan=None,stratagy=None,sl_percent=None) :

#     def day_start(stratagy):
#         count = 0
#         while True:
#             entry_id= client['order_adjuster'] # Database Name
#             pending_order,filled_orders  = order_book()
#             pending_orders_db  = pd.DataFrame(entry_id[stratagy].find())
#             pending_orders_db = pending_orders_db[pending_orders_db['status'] =='pending']

#             if len(pending_orders_db)!=0:
#                 # print(pending_orders_db)

#                 for index,row in pending_orders_db.iterrows():
#                     if (row['order_id']  in filled_orders['orderId'].tolist()):
#                         entry_id[stratagy].update_one({ "order_id": row['order_id']}, { "$set": { "status": "placed_sucessfully" } }) # it will update if sl hit  modificarion status to slhit
#                         alert_bot(f'{row["order_id"]} :  placed_sucessfully')
#                         time.sleep(1)

#                     elif count<3:
#                         # modify order by price here
#                         ltp = get_ltp(row['token'])
#                         price = row['price']

#                         if ltp>price:
#                             new_price  =round(abs(price)-abs(ltp-price)/2,1)
#                         else:
#                             new_price  =round(abs(price)+abs(ltp-price)/2,1)

#                         try:
#                             responce = kotak.modify_order(order_id = str(row['order_id']), price =new_price , quantity = row['qty'], disclosed_quantity = 0, trigger_price = new_price+0.05, validity = "GFD")
#                             alert_bot(f"Entry price modified \nMessage :{responce['Success']['NSE']['message']} \nPrice : {new_price}")
#                             entry_id[stratagy].update_one({ "order_id": row['order_id']}, { "$set": { "price": new_price } }) # it will update updated price to database
#                             count =count + 1
#                             time.sleep(5)
#                         except Exception as e:
#                             emergency_bot(f'Not able to modify sl in Smart Execuator(kotak.modify_order)\nMessage : {e}')

#                     else:
#                         try:
#                             responce = kotak.modify_order(order_id = str(row['order_id']), price =0 , quantity = row['qty'], disclosed_quantity = 0, trigger_price = 0, validity = "GFD")
#                             alert_bot(f"Executed at Market Order \nMessage :{responce['Success']['NSE']['message']}")
#                         except Exception as e:
#                             # print(f'Not able to modify sl in Smart Executer SOD Market Order \nMessage : {e}')
#                             emergency_bot(f'Not able to modify sl in Smart Executer SOD Market Order\nMessage : {e}')

#                     time.sleep(5)


#             else:

#                 def place_sl(stratagy,sl_percent):
#                     pending_order,Filled_orders = order_book()
#                     # print('-----------------------  SL Orders -----------------------')
#                     for index,row in (Filled_orders.iloc[-2:]).iterrows():
#                         place_limit_sl(row['instrumentToken'],row['orderQuantity'],row['transactionType'],row['price'],sl_percent)

#                 def store_sl(stratagy):
#                     pending_order,Filled_orders = order_book()
#                     pendings = pending_order[-2:]
#                     Filled = Filled_orders[-2:][['instrumentToken','price','orderQuantity','transactionType','orderId']]
#                     pending_ = pendings.merge(Filled,how='outer',on='instrumentToken').dropna()
#                     pending_with_tradedprice = pending_[['orderId_x','instrumentToken','price_x','price_y','orderQuantity_x','orderId_y']]

#                     for index,row in pending_with_tradedprice.iterrows():
#                         todays_tickers[stratagy].insert_one({'order_id':row['orderId_x'],'instrumentToken':row['instrumentToken'],'sl_price':row['price_x'],'tradedPrice':row['price_y'],'qty':row['orderQuantity_x'],'status':'None','filled_id':row['orderId_y']})

#                 place_sl(stratagy,sl_percent)
#                 store_sl(stratagy)
#                 break

#     def day_end():

#         while True:
#             pending_order,Filled_orders = order_book()
#             all_positions,open_positions,pl = positions()
#             pending_order = pending_order.merge(open_positions,on='instrumentToken',how ='inner')

#             if len(pending_order)!=0:
#                 time.sleep(5)
#                 for index,row in pending_order.iterrows():
#                     if (row['orderId'] not in Filled_orders['orderId'].tolist()):
#                         price = get_ltp(row['instrumentToken'])
#                         try:
#                             responce = kotak.modify_order(order_id = str(row['orderId']), price =price , quantity = row['orderQuantity'], disclosed_quantity = 0, trigger_price = price-0.05, validity = "GFD")
#                             alert_bot(f"Order Eod Exicuation \nMessage :{responce['Success']['NSE']['message']} \nPrice : {price}")
#                         except Exception as e:
#                             emergency_bot(f'Not able to modify sl in Smart Execuator\nMessage : {e}')

#             else:
#                 def cancel_all_pending_orders():
#                     pending_orders,Filled_orders = order_book()
#                     if len(pending_orders)>0:
#                         pending_ = pending_orders['orderId'].to_list()
#                         for i in pending_:
#                             try:
#                                 responce = (kotak.cancel_order(order_id = i))
#                                 alert_bot(f"Pending order canceled \nMessage : {responce['Success']['NSE']['message']}")

#                             except Exception as e:
#                                 emergency_bot(f"Not able to cancel pending order \nMessage : {e}")

#                 def exit_all_intraday_orders():
#                     all_positions,open_positions,pl = positions()
#                     for index,row in open_positions.iterrows():
#                         if row['netTrdQtyLot'] >0:
#                             side = 'SELL'
#                             qty = row['netTrdQtyLot']

#                         elif row['netTrdQtyLot'] <0:
#                             side = 'BUY'
#                             qty = -row['netTrdQtyLot']
#                         else:
#                             pass
#                         try:
#                             responce = kotak.place_order(order_type = "MIS", instrument_token = row['instrumentToken'], transaction_type = side,
#                                     quantity = qty, price = 0, disclosed_quantity = 0, trigger_price = 0,
#                                     tag = "string", validity = "GFD", variety = "REGULAR")
#                             # print(f"Hedges removed Sucessfully !!! \nMessage :{responce['Success']['NSE']['message']}")
#                             alert_bot(f"Hedges removed Sucessfully !!! \nMessage :{responce['Success']['NSE']['message']}")

#                             time.sleep(2)
#                         except Exception as e:
#                             # print(f'Not able to modify sl in Smart Execuator\nMessage : {e}')
#                             emergency_bot(f'Not able to modify sl in Smart Execuator\nMessage : {e}')

#                 def clear_db():
#                     entry_id= client['order_adjuster']
#                     todays_tickers = client['todays_tickers']
#                     todays_tickers['nine_thirty'].delete_many({})
#                     todays_tickers['nine_twenty'].delete_many({})
#                     entry_id['nine_thirty'].delete_many({})
#                     entry_id['nine_twenty'].delete_many({})


#                 try:
#                     cancel_all_pending_orders()
#                     time.sleep(5)
#                 except Exception as e:
#                     # print(e)
#                     emergency_bot(f'Problem in cancel_all_pending_orders()\nMessage : {e}')

#                 try:
#                     exit_all_intraday_orders()
#                     time.sleep(2)
#                 except Exception as e:
#                     # print(e)
#                     emergency_bot(f'Problem in exit_all_intraday_orders()\nMessage : {e}')

#                 try:
#                     clear_db()
#                 except Exception as e:
#                     # print(e)
#                     emergency_bot(f'Problem in clear_db()\nMessage : {e}')

#                 break

#     if plan=='day_entry_orders':
#         day_start(stratagy)

#     elif plan=='day_exit_orders':
#         day_end()

# # ------------   Checking   ------------------------

# def day_tracker(pl):
#     try:
#         todays_tickers = client['day_tracker']
#         todays_tickers[dt.strftime(dt.today().date(),'%d/%m/%Y')].insert_one({"Time": get_ist_now().time().strftime('%H:%M') ,"P/L" : round(pl,2)})
#     except Exception as e :
#         emergency_bot(f"Problem in : day_tracker(pl) \nMessage : {e}" )

# def sl_mamangement(stratagy,pending_orders):
#     todays_ = todays_tickers[stratagy]
#     db = pd.DataFrame(todays_.find())
#     for index,row in db.iterrows():
#         if (row['status'] not in ['reentry_pending','reenterd']) and (row['order_id'] not in pending_orders['orderId'].tolist()):
#             try:
#                 price = row['tradedPrice']
#                 responce = kotak.place_order(order_type = "MIS", instrument_token = row['instrumentToken'], transaction_type = 'SELL',
#                                     quantity = row['qty'], price = price, disclosed_quantity = 0, trigger_price = price+0.05,
#                                     tag = "string", validity = "GFD", variety = "REGULAR")
#                 time.sleep(5)
#                 pending_order,filled_orders = order_book()
#                 reentrd_sl_id = pending_order.iloc[-1]['orderId']
#                 # print(reentrd_sl_id,"----",row['instrumentToken'])
#                 todays_.update_one({ "instrumentToken":row['instrumentToken']}, { "$set": { "order_id": int(reentrd_sl_id)} })
#                 todays_.update_one({ "instrumentToken":row['instrumentToken']}, { "$set": { "status": 'reentry_pending'} })
#                 # print(f"\nRentry Pending Order Placed  SucessFully !!! \nmessage : {responce['Success']['NSE']['message']}")
#                 alert_bot(f"Rentry Pending Order Placed  SucessFully !!! \nMessage : {responce['Success']['NSE']['message']} \nPrice : {price}")

#             except Exception as e:
#                 print(f'Problem in sl_mamangement reentry_pending \nreason{e}')
#                 emergency_bot(f'Problem in sl_mamangement reentry_pending \nreason{e}')

#     reentry_pending_db = db[db['status']=='reentry_pending']
#     if len(reentry_pending_db)>0:
#         for index,row in reentry_pending_db.iterrows() :
#             if (row['order_id'] not in pending_orders['orderId'].tolist()):
#                 try:
#                     price = row['sl_price']
#                     responce = kotak.place_order(order_type = "MIS", instrument_token = row['instrumentToken'], transaction_type = 'BUY',
#                                         quantity = row['qty'], price = price, disclosed_quantity = 0, trigger_price = price-0.05,
#                                         tag = "string", validity = "GFD", variety = "REGULAR")
#                     time.sleep(5)
#                     pending_order,filled_orders = order_book()
#                     reentrd_sl_id = pending_order.iloc[-1]['orderId']
#                     todays_.update_one({ "instrumentToken":row['instrumentToken']}, { "$set": { "order_id": int(reentrd_sl_id)} })
#                     todays_.update_one({ "instrumentToken":row['instrumentToken']}, { "$set": { "status": 'reenterd'} })
#                     # print(f"\nRentry sl Placed  SucessFully !!! \nmessage : {responce['Success']['NSE']['message']}")
#                     alert_bot(f"Rentry sl Placed  SucessFully !!! \nMessage : {responce['Success']['NSE']['message']} Price : {price}")

#                 except Exception as e:
#                     # print(f'Problem in sl_mamangement reenterd \nreason{e}')
#                     emergency_bot(f'Problem in sl_mamangement reenterd \nMessage : {e}')

# def is_loss_above_limit(pl):
#     if  pl<(-acoount_balance*(allowed_loss_percent/100)):
#         emergency_bot(f'Loss is above the limit Please Check it \nP/l : {pl}')

# def placing():

#     current_time = get_ist_now()

#     if dt.strftime(current_time,'%H:%M')==first_order:
#         alert_bot (f'------------------- \nFirst Order\n------------------  ')
#         tickers,buy_hedges,hedges= get_todays_ticker(150,buy_hedges =True)
#         for i in tickers:
#             (order_place(i,15,side='SELL',stratagy = 'nine_twenty',sl_percent = 50),'\n')  # Place order one by one
#         smart_executer(plan="day_entry_orders",stratagy = 'nine_twenty',sl_percent = 50)
#         if buy_hedges ==True:
#             for i in hedges:
#                 (order_place(i,45,side='BUY',stratagy = 'hedges',sl_percent = None),'\n')  # Place order one by one


#     elif dt.strftime(current_time,'%H:%M')==second_order:
#         alert_bot (f'------------------- \nSecond Order\n ------------------ ')
#         tickers,buy_hedges,hedges= get_todays_ticker(125,buy_hedges = True)
#         for i in tickers:
#             (order_place(i,30,side='SELL',stratagy = 'nine_thirty',sl_percent = 25),'\n')  # Place order one by one
#         smart_executer(plan="day_entry_orders",stratagy = 'nine_thirty',sl_percent = 25)


#     elif (dt.strftime(current_time,'%H:%M')==exit_order)or(dt.strftime(current_time,'%H:%M')=='15:16'):   #time in 24hrs
#         alert_bot (f'------------------- \nEod Orders\n------------------ ')
#         try:
#             smart_executer(plan="day_exit_orders")


#         except Exception as e:
#             emergency_bot(f"Problem in : day_exit_orders \nerror : {e}" )


#     else :
#         pass

# def checking():

#     try:
#         all_positions,open_positions,pl = positions()
#         pending_orders,Filled_orders= order_book()

#         day_tracker(pl)
#         is_loss_above_limit(pl)
#         sl_mamangement("nine_thirty",pending_orders)
#         logger_bot(f' P/L : {pl}')

#     except KeyError:
#         pass

#     except Exception as e:
#         error = str(e)
#         num = (error.find('You have been logged out of your account due to inactivity or have logged into another device'))
#         if num ==144:
#             logger_bot('---  Login Crossed ---')

#         else:
#             emergency_bot(f'problem in broker server \nMessage : {e}')

# def lambda_handler(event=None, context=None):


#     while True:
#         current_time = get_ist_now()
#         if start_time < current_time.time() < end_time:
#             current_time = get_ist_now().second

#             placing()
#             checking()


#             wait_to_align = 60.0 - current_time
#             time.sleep(wait_to_align)

#         else :
#             pass

# lambda_handler(event=None, context=None)