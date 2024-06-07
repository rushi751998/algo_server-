
import requests
from  datetime import datetime as dt
from  datetime import datetime as dt,timedelta
import os
from dotenv import load_dotenv

load_dotenv() 
def get_ist_now():
    return dt.now() + timedelta(0)

def emergency_bot(bot_message):
    """ It is used for sending alert to Emergeny situation"""
    current_time = get_ist_now()
    bot_message = f'{dt.strftime(current_time,"%H:%M:%S")}\n{bot_message}'
    bot_token = os.environ['emergency_bot_token']
    bot_chatId = os.environ['chatId']
    print(bot_message,'\n')
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatId + '&parse_mode=Markdown&text=' + bot_message
    response = requests.get(send_text)
    if response.status_code != 200:
        alert_bot("emergency_bot not able to send message")
        

def alert_bot(bot_message : str,send_image : bool = False):
    """ It is used for sending price alert"""
    current_time = get_ist_now()
    bot_message = f'{dt.strftime(current_time,"%H:%M:%S")}\n{bot_message}'
    bot_token = os.environ['alert_bot_token']
    bot_chatId = os.environ['chatId']
    
    if send_image : 
        with open('plot.png', 'rb') as file:
            # Send the image
            response = requests.post(url, data={'chat_id': CHAT_ID}, files={'photo': file})
            if response.status_code != 200:
                emergency_bot("alert_bot not able to send image")
    else : 
        print(bot_message,'\n')

        send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatId + '&parse_mode=Markdown&text=' + bot_message
        response = requests.get(send_text)

def logger_bot(bot_message):
    """ It is used for sending order manangemant"""
    current_time = get_ist_now()
    bot_message = f'{dt.strftime(current_time,"%H:%M:%S")} {bot_message}'
    bot_token = os.environ['logger_bot_token']
    bot_chatId = os.environ['chatId']
    print(bot_message,'\n')
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatId + '&parse_mode=Markdown&text=' + bot_message
    response = requests.get(send_text)
    if response.status_code != 200:
        emergency_bot("logger_bot not able to send message")


def Trigger_finder(bot_message):
    """ It is used for sending alert to Emergeny situation"""
    # current_time = get_ist_now()
    # bot_message = f'{dt.strftime(current_time,"%H:%M:%S")}\n{bot_message}'
    bot_token = os.environ['Trigger_finder_token']
    bot_chatId = os.environ['chatId']
    print(bot_message,'\n')
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatId + '&parse_mode=Markdown&text=' + bot_message
    response = requests.get(send_text)
    if response.status_code != 200:
        emergency_bot("Trigger_finder not able to send message")