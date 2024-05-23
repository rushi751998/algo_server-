import neo_api_client
from engine import login_Engine
from telegram_bot import logger_bot,emergency_bot
import pandas as pd
from  datetime import datetime as dt
from dateutil.relativedelta import relativedelta
import time
import threading
from utils import set_coloumn_name, order_staus_dict, env_variables as env, Fields as F
nan = 'nan'

option_chain = {}
ticker_to_token= {}

# option_chain = {'41941': {'v': 0, 'oi': 210, 'ltp': 3465.3, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452252500PE'}, '39845': {'v': 28065, 'oi': 37155, 'ltp': 1.0, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452244200PE'}, '39848': {'v': 0, 'oi': 45, 'ltp': 4717.35, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452244300CE'}, '39849': {'v': 56385, 'oi': 91095, 'ltp': 1.15, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452244300PE'}, '39850': {'v': 0, 'oi': 150, 'ltp': 3468.0, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452244400CE'}, '39855': {'v': 67950, 'oi': 118920, 'ltp': 1.05, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452244400PE'}, '39877': {'v': 60, 'oi': 840, 'ltp': 3485.65, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452244500CE'}, '39878': {'v': 713955, 'oi': 417960, 'ltp': 1.15, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452244500PE'}, '39879': {'v': 0, 'oi': 75, 'ltp': 3004.4, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452244600CE'}, '39881': {'v': 164295, 'oi': 191490, 'ltp': 1.0, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452244600PE'}, '39882': {'v': 0, 'oi': 60, 'ltp': 3346.0, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452244700CE'}, '39844': {'v': 0, 'oi': 150, 'ltp': 4827.75, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452244200CE'}, '39883': {'v': 361500, 'oi': 204195, 'ltp': 1.1, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452244700PE'}, '39885': {'v': 312795, 'oi': 220290, 'ltp': 1.15, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452244800PE'}, '39886': {'v': 0, 'oi': 30, 'ltp': 4126.95, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452244900CE'}, '39898': {'v': 389760, 'oi': 255510, 'ltp': 1.1, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452244900PE'}, '44300': {'v': 78465, 'oi': 82875, 'ltp': 0.95, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452255500CE'}, '44303': {'v': 0, 'oi': 0, 'ltp': 0.0, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452255500PE'}, '41537': {'v': 6550950, 'oi': 1723830, 'ltp': 20.9, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452247000PE'}, '41533': {'v': 112785, 'oi': 180630, 'ltp': 1015.3, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452247000CE'}, '41532': {'v': 2918325, 'oi': 602550, 'ltp': 16.15, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452246900PE'}, '39900': {'v': 0, 'oi': 2835, 'ltp': 3086.25, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452245000CE'}, '39901': {'v': 2306610, 'oi': 1271340, 'ltp': 1.4, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452245000PE'}, '39884': {'v': 0, 'oi': 30, 'ltp': 4210.65, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452244800CE'}, '39902': {'v': 0, 'oi': 15, 'ltp': 2845.45, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452245100CE'}, '39843': {'v': 24420, 'oi': 40575, 'ltp': 1.05, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452244100PE'}, '41538': {'v': 11700, 'oi': 24045, 'ltp': 921.15, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452247100CE'}, '41617': {'v': 14554320, 'oi': 3430860, 'ltp': 230.25, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452248000CE'}, '41616': {'v': 8330115, 'oi': 1415220, 'ltp': 193.3, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452247900PE'}, '41615': {'v': 5649435, 'oi': 1068300, 'ltp': 285.7, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452247900CE'}, '41610': {'v': 7473060, 'oi': 1463175, 'ltp': 154.85, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452247800PE'}, '41577': {'v': 2857155, 'oi': 696120, 'ltp': 348.1, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452247800CE'}, '41576': {'v': 6014850, 'oi': 1327665, 'ltp': 123.2, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452247700PE'}, '41574': {'v': 1009350, 'oi': 564405, 'ltp': 415.8, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452247700CE'}, '41573': {'v': 5940450, 'oi': 872565, 'ltp': 97.1, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452247600PE'}, '41572': {'v': 384945, 'oi': 196245, 'ltp': 491.15, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452247600CE'}, '39709': {'v': 15, 'oi': 375, 'ltp': 4125.3, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452244000CE'}, '39840': {'v': 0, 'oi': 75, 'ltp': 3802.8, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452244100CE'}, '39710': {'v': 489090, 'oi': 416880, 'ltp': 1.15, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452244000PE'}, '41568': {'v': 624465, 'oi': 332985, 'ltp': 570.0, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452247500CE'}, '41561': {'v': 3743430, 'oi': 736275, 'ltp': 58.95, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452247400PE'}, '41558': {'v': 64710, 'oi': 95850, 'ltp': 651.05, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452247400CE'}, '41557': {'v': 3261405, 'oi': 713760, 'ltp': 45.6, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452247300PE'}, '41556': {'v': 39090, 'oi': 63015, 'ltp': 738.85, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452247300CE'}, '44297': {'v': 155610, 'oi': 341340, 'ltp': 0.85, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452255000CE'}, '44299': {'v': 0, 'oi': 0, 'ltp': 0.0, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452255000PE'}, '41555': {'v': 2983035, 'oi': 729735, 'ltp': 35.1, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452247200PE'}, '41553': {'v': 30870, 'oi': 37845, 'ltp': 831.0, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452247200CE'}, '41552': {'v': 2195505, 'oi': 568920, 'ltp': 27.1, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452247100PE'}, '41569': {'v': 8054610, 'oi': 1924605, 'ltp': 75.6, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452247500PE'}, '39903': {'v': 428775, 'oi': 250230, 'ltp': 1.35, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452245100PE'}, '39910': {'v': 577035, 'oi': 308805, 'ltp': 1.4, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452245200PE'}, '39927': {'v': 0, 'oi': 165, 'ltp': 2422.1, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452245300CE'}, '36449': {'v': 0, 'oi': 0, 'ltp': 0.0, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452241000CE'}, '36448': {'v': 65925, 'oi': 29670, 'ltp': 0.75, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452240500PE'}, '36444': {'v': 0, 'oi': 0, 'ltp': 0.0, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452240500CE'}, '45641': {'v': 50385, 'oi': 121710, 'ltp': 0.75, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452256000CE'}, '45642': {'v': 0, 'oi': 15, 'ltp': 8450.0, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452256000PE'}, '45647': {'v': 55755, 'oi': 58395, 'ltp': 0.75, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452256500CE'}, '45648': {'v': 0, 'oi': 0, 'ltp': 0.0, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452256500PE'}, '45650': {'v': 368940, 'oi': 1094610, 'ltp': 0.75, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452257000CE'}, '45691': {'v': 0, 'oi': 0, 'ltp': 0.0, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452257000PE'}, '36272': {'v': 306915, 'oi': 94965, 'ltp': 0.8, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452240000PE'}, '36450': {'v': 40125, 'oi': 48735, 'ltp': 0.75, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452241000PE'}, '36271': {'v': 0, 'oi': 0, 'ltp': 0.0, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452240000CE'}, '36269': {'v': 0, 'oi': 0, 'ltp': 0.0, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452239500CE'}, '40700': {'v': 1569255, 'oi': 584310, 'ltp': 2.35, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452245900PE'}, '40699': {'v': 120, 'oi': 630, 'ltp': 2090.0, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452245900CE'}, '40697': {'v': 1112085, 'oi': 565725, 'ltp': 2.15, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452245800PE'}, '40691': {'v': 0, 'oi': 720, 'ltp': 2251.05, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452245800CE'}, '40690': {'v': 913425, 'oi': 393750, 'ltp': 2.1, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452245700PE'}, '40689': {'v': 0, 'oi': 1215, 'ltp': 2392.95, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452245700CE'}, '40523': {'v': 928755, 'oi': 356070, 'ltp': 1.8, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452245600PE'}, '40520': {'v': 0, 'oi': 15, 'ltp': 2357.0, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452245600CE'}, '41940': {'v': 547425, 'oi': 585600, 'ltp': 1.6, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452252500CE'}, '36270': {'v': 234195, 'oi': 417270, 'ltp': 0.75, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452239500PE'}, '36451': {'v': 0, 'oi': 0, 'ltp': 0.0, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452241500CE'}, '36454': {'v': 185655, 'oi': 244980, 'ltp': 0.6, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452241500PE'}, '41026': {'v': 5535, 'oi': 13170, 'ltp': 1999.0, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452246000CE'}, '39928': {'v': 502560, 'oi': 411030, 'ltp': 1.5, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452245300PE'}, '39929': {'v': 0, 'oi': 180, 'ltp': 2551.8, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452245400CE'}, '39930': {'v': 884280, 'oi': 250935, 'ltp': 1.65, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452245400PE'}, '39932': {'v': 60, 'oi': 2160, 'ltp': 2497.8, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452245500CE'}, '39939': {'v': 4061280, 'oi': 1943910, 'ltp': 1.85, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452245500PE'}, '41529': {'v': 3525, 'oi': 11190, 'ltp': 1088.5, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452246900CE'}, '41164': {'v': 2336055, 'oi': 804225, 'ltp': 12.3, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452246800PE'}, '41163': {'v': 3375, 'oi': 13860, 'ltp': 1200.0, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452246800CE'}, '41159': {'v': 2069865, 'oi': 660585, 'ltp': 9.6, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452246700PE'}, '41154': {'v': 3210, 'oi': 17115, 'ltp': 1317.05, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452246700CE'}, '41137': {'v': 2073060, 'oi': 536595, 'ltp': 7.75, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452246600PE'}, '41136': {'v': 495, 'oi': 7470, 'ltp': 1367.2, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452246600CE'}, '41120': {'v': 5830320, 'oi': 1557450, 'ltp': 6.35, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452246500PE'}, '41119': {'v': 5610, 'oi': 22560, 'ltp': 1487.35, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452246500CE'}, '41081': {'v': 2133345, 'oi': 545715, 'ltp': 5.15, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452246400PE'}, '41068': {'v': 390, 'oi': 3855, 'ltp': 1606.0, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452246400CE'}, '41065': {'v': 3010845, 'oi': 786735, 'ltp': 4.15, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452246300PE'}, '41064': {'v': 60, 'oi': 1830, 'ltp': 1644.85, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452246300CE'}, '41043': {'v': 2130255, 'oi': 675705, 'ltp': 3.55, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452246200PE'}, '41042': {'v': 360, 'oi': 3180, 'ltp': 1805.0, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452246200CE'}, '41033': {'v': 1892580, 'oi': 459285, 'ltp': 3.2, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452246100PE'}, '41029': {'v': 75, 'oi': 960, 'ltp': 1950.0, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452246100CE'}, '41028': {'v': 6531240, 'oi': 2372610, 'ltp': 2.9, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452246000PE'}, '41620': {'v': 20239560, 'oi': 2795970, 'ltp': 237.9, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452248000PE'}, '41621': {'v': 13791900, 'oi': 2419830, 'ltp': 181.75, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452248100CE'}, '39905': {'v': 0, 'oi': 150, 'ltp': 2572.5, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452245200CE'}, '41639': {'v': 13281075, 'oi': 2680305, 'ltp': 141.0, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452248200CE'}, '42005': {'v': 320220, 'oi': 384000, 'ltp': 1.25, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452253000CE'}, '42008': {'v': 0, 'oi': 225, 'ltp': 5021.2, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452253000PE'}, '42018': {'v': 34020, 'oi': 32580, 'ltp': 1.35, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452253500CE'}, '42019': {'v': 0, 'oi': 90, 'ltp': 5141.4, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452253500PE'}, '42022': {'v': 31935, 'oi': 199575, 'ltp': 1.15, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452254000CE'}, '42024': {'v': 0, 'oi': 165, 'ltp': 5647.25, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452254000PE'}, '42025': {'v': 20895, 'oi': 62745, 'ltp': 1.1, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452254500CE'}, '42026': {'v': 0, 'oi': 60, 'ltp': 5116.05, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452254500PE'}, '37654': {'v': 0, 'oi': 0, 'ltp': 0.0, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452242000CE'}, '37655': {'v': 153480, 'oi': 199065, 'ltp': 0.75, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452242000PE'}, '41852': {'v': 194820, 'oi': 55035, 'ltp': 2.05, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452251200CE'}, '41850': {'v': 185430, 'oi': 51510, 'ltp': 2.0, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452251100CE'}, '41848': {'v': 2109840, 'oi': 980400, 'ltp': 1.9, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452251000CE'}, '41847': {'v': 0, 'oi': 120, 'ltp': 3467.2, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452250900PE'}, '41846': {'v': 255720, 'oi': 139020, 'ltp': 2.05, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452250900CE'}, '41839': {'v': 0, 'oi': 105, 'ltp': 2899.05, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452250800PE'}, '41838': {'v': 440505, 'oi': 240360, 'ltp': 2.05, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452250800CE'}, '41837': {'v': 0, 'oi': 45, 'ltp': 2592.95, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452250700PE'}, '41836': {'v': 370830, 'oi': 144135, 'ltp': 2.2, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452250700CE'}, '41831': {'v': 0, 'oi': 15, 'ltp': 2583.05, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452250600PE'}, '41795': {'v': 398445, 'oi': 158685, 'ltp': 2.3, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452250600CE'}, '41793': {'v': 0, 'oi': 630, 'ltp': 2400.0, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452250500PE'}, '41849': {'v': 195, 'oi': 660, 'ltp': 3026.25, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452251000PE'}, '41853': {'v': 0, 'oi': 135, 'ltp': 3375.55, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452251200PE'}, '41854': {'v': 151845, 'oi': 44160, 'ltp': 2.0, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452251300CE'}, '41859': {'v': 0, 'oi': 360, 'ltp': 3494.8, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452251300PE'}, '41936': {'v': 0, 'oi': 60, 'ltp': 3372.1, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452252400PE'}, '41934': {'v': 25650, 'oi': 26745, 'ltp': 1.65, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452252400CE'}, '41933': {'v': 0, 'oi': 60, 'ltp': 3267.9, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452252300PE'}, '41932': {'v': 51915, 'oi': 25350, 'ltp': 1.7, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452252300CE'}, '41931': {'v': 0, 'oi': 195, 'ltp': 4339.3, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452252200PE'}, '41930': {'v': 67800, 'oi': 34260, 'ltp': 1.7, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452252200CE'}, '41929': {'v': 0, 'oi': 120, 'ltp': 3089.3, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452252100PE'}, '41928': {'v': 38940, 'oi': 48120, 'ltp': 1.7, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452252100CE'}, '41926': {'v': 0, 'oi': 600, 'ltp': 3850.0, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452252000PE'}, '41638': {'v': 15204165, 'oi': 1528350, 'ltp': 289.9, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452248100PE'}, '41924': {'v': 600615, 'oi': 611460, 'ltp': 1.6, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452252000CE'}, '41923': {'v': 0, 'oi': 60, 'ltp': 2896.95, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452251900PE'}, '41922': {'v': 234855, 'oi': 168690, 'ltp': 1.75, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452251900CE'}, '41919': {'v': 0, 'oi': 135, 'ltp': 2806.9, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452251800PE'}, '41909': {'v': 146970, 'oi': 143715, 'ltp': 1.85, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452251800CE'}, '41907': {'v': 0, 'oi': 150, 'ltp': 3837.25, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452251700PE'}, '41906': {'v': 210090, 'oi': 185670, 'ltp': 1.85, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452251700CE'}, '41900': {'v': 0, 'oi': 105, 'ltp': 2608.35, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452251600PE'}, '41887': {'v': 185625, 'oi': 112800, 'ltp': 1.9, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452251600CE'}, '41886': {'v': 0, 'oi': 165, 'ltp': 3389.2, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452251500PE'}, '41877': {'v': 774960, 'oi': 582540, 'ltp': 1.8, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452251500CE'}, '41874': {'v': 0, 'oi': 30, 'ltp': 2407.25, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452251400PE'}, '41862': {'v': 133590, 'oi': 81180, 'ltp': 2.0, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452251400CE'}, '41790': {'v': 2998935, 'oi': 1322250, 'ltp': 2.35, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452250500CE'}, '41787': {'v': 0, 'oi': 210, 'ltp': 2700.0, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452250400PE'}, '41851': {'v': 0, 'oi': 360, 'ltp': 3285.45, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452251100PE'}, '41786': {'v': 742260, 'oi': 387825, 'ltp': 2.5, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452250400CE'}, '41717': {'v': 735, 'oi': 3690, 'ltp': 1325.6, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452249300PE'}, '41714': {'v': 4067190, 'oi': 679410, 'ltp': 6.7, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452249300CE'}, '41712': {'v': 10620, 'oi': 18150, 'ltp': 1223.0, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452249200PE'}, '41711': {'v': 5669520, 'oi': 1236435, 'ltp': 7.95, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452249200CE'}, '41710': {'v': 9990, 'oi': 6135, 'ltp': 1119.95, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452249100PE'}, '41699': {'v': 4194060, 'oi': 754995, 'ltp': 10.05, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452249100CE'}, '41698': {'v': 105000, 'oi': 187335, 'ltp': 1010.2, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452249000PE'}, '41695': {'v': 8485980, 'oi': 2140590, 'ltp': 13.15, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452249000CE'}, '41694': {'v': 31275, 'oi': 18750, 'ltp': 910.4, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452248900PE'}, '41680': {'v': 4718190, 'oi': 967320, 'ltp': 17.2, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452248900CE'}, '41718': {'v': 3515670, 'oi': 593625, 'ltp': 5.55, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452249400CE'}, '41679': {'v': 45765, 'oi': 38070, 'ltp': 828.9, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452248800PE'}, '41677': {'v': 177150, 'oi': 30945, 'ltp': 730.95, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452248700PE'}, '41671': {'v': 386865, 'oi': 76200, 'ltp': 651.1, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452248600PE'}, '41667': {'v': 6205770, 'oi': 979725, 'ltp': 43.7, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452248600CE'}, '41666': {'v': 1730370, 'oi': 331095, 'ltp': 566.75, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452248500PE'}, '41653': {'v': 10909020, 'oi': 2697615, 'ltp': 59.35, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452248500CE'}, '41647': {'v': 2560065, 'oi': 241530, 'ltp': 488.7, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452248400PE'}, '41646': {'v': 6309345, 'oi': 1475955, 'ltp': 80.4, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452248400CE'}, '41645': {'v': 4930755, 'oi': 441105, 'ltp': 414.3, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452248300PE'}, '41644': {'v': 7774065, 'oi': 1755000, 'ltp': 107.55, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452248300CE'}, '41642': {'v': 12761700, 'oi': 1088775, 'ltp': 348.85, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452248200PE'}, '41678': {'v': 4793220, 'oi': 920745, 'ltp': 23.1, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452248800CE'}, '41719': {'v': 570, 'oi': 4050, 'ltp': 1429.9, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452249400PE'}, '41674': {'v': 5345085, 'oi': 1014285, 'ltp': 31.75, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452248700CE'}, '41729': {'v': 4425, 'oi': 24000, 'ltp': 1500.0, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452249500PE'}, '41760': {'v': 1034190, 'oi': 514440, 'ltp': 2.9, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452250100CE'}, '41759': {'v': 1245, 'oi': 48015, 'ltp': 2041.55, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452250000PE'}, '41758': {'v': 6744000, 'oi': 3301575, 'ltp': 3.15, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452250000CE'}, '41757': {'v': 45, 'oi': 855, 'ltp': 1931.8, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452249900PE'}, '41780': {'v': 0, 'oi': 135, 'ltp': 2350.0, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452250200PE'}, '41781': {'v': 695985, 'oi': 508620, 'ltp': 2.6, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452250300CE'}, '41720': {'v': 6643635, 'oi': 2091735, 'ltp': 4.85, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452249500CE'}, '41783': {'v': 0, 'oi': 165, 'ltp': 2399.3, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452250300PE'}, '41761': {'v': 15, 'oi': 30, 'ltp': 2121.0, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452250100PE'}, '41754': {'v': 1513950, 'oi': 589080, 'ltp': 3.35, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452249900CE'}, '41751': {'v': 15, 'oi': 570, 'ltp': 1938.05, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452249800PE'}, '41762': {'v': 933630, 'oi': 848490, 'ltp': 2.75, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452250200CE'}, '41749': {'v': 60, 'oi': 1095, 'ltp': 1725.0, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452249700PE'}, '38863': {'v': 456300, 'oi': 338295, 'ltp': 1.0, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452243500PE'}, '38862': {'v': 0, 'oi': 90, 'ltp': 3900.0, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452243500CE'}, '41738': {'v': 2612715, 'oi': 624810, 'ltp': 4.2, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452249600CE'}, '41739': {'v': 375, 'oi': 2145, 'ltp': 1592.8, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452249600PE'}, '38806': {'v': 226320, 'oi': 355845, 'ltp': 1.1, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452243000PE'}, '41750': {'v': 1972710, 'oi': 608145, 'ltp': 3.5, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452249800CE'}, '38805': {'v': 0, 'oi': 90, 'ltp': 5008.45, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452243000CE'}, '38420': {'v': 0, 'oi': 0, 'ltp': 0.0, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452242500CE'}, '41742': {'v': 2091585, 'oi': 706470, 'ltp': 3.8, 'option_type': 'CE', 'symbol': 'BANKNIFTY2452249700CE'}, '38421': {'v': 217725, 'oi': 278835, 'ltp': 1.0, 'option_type': 'PE', 'symbol': 'BANKNIFTY2452242500PE'}, '46923': {'v': 340830, 'oi': 2358435, 'ltp': 48140.0, 'option_type': 'XX', 'symbol': 'BANKNIFTY24MAYFUT'}, '35020': {'v': 66495, 'oi': 425985, 'ltp': 48450.0, 'option_type': 'XX', 'symbol': 'BANKNIFTY24JUNFUT'}, '35165': {'v': 6225, 'oi': 94680, 'ltp': 48754.8, 'option_type': 'XX', 'symbol': 'BANKNIFTY24JULFUT'}}


# ticker_to_token = {'BANKNIFTY2452252500PE': '41941', 'BANKNIFTY2452244200PE': '39845', 'BANKNIFTY2452244300CE': '39848', 'BANKNIFTY2452244300PE': '39849', 'BANKNIFTY2452244400CE': '39850', 'BANKNIFTY2452244400PE': '39855', 'BANKNIFTY2452244500CE': '39877', 'BANKNIFTY2452244500PE': '39878', 'BANKNIFTY2452244600CE': '39879', 'BANKNIFTY2452244600PE': '39881', 'BANKNIFTY2452244700CE': '39882', 'BANKNIFTY2452244200CE': '39844', 'BANKNIFTY2452244700PE': '39883', 'BANKNIFTY2452244800PE': '39885', 'BANKNIFTY2452244900CE': '39886', 'BANKNIFTY2452244900PE': '39898', 'BANKNIFTY2452255500CE': '44300', 'BANKNIFTY2452255500PE': '44303', 'BANKNIFTY2452247000PE': '41537', 'BANKNIFTY2452247000CE': '41533', 'BANKNIFTY2452246900PE': '41532', 'BANKNIFTY2452245000CE': '39900', 'BANKNIFTY2452245000PE': '39901', 'BANKNIFTY2452244800CE': '39884', 'BANKNIFTY2452245100CE': '39902', 'BANKNIFTY2452244100PE': '39843', 'BANKNIFTY2452247100CE': '41538', 'BANKNIFTY2452248000CE': '41617', 'BANKNIFTY2452247900PE': '41616', 'BANKNIFTY2452247900CE': '41615', 'BANKNIFTY2452247800PE': '41610', 'BANKNIFTY2452247800CE': '41577', 'BANKNIFTY2452247700PE': '41576', 'BANKNIFTY2452247700CE': '41574', 'BANKNIFTY2452247600PE': '41573', 'BANKNIFTY2452247600CE': '41572', 'BANKNIFTY2452244000CE': '39709', 'BANKNIFTY2452244100CE': '39840', 'BANKNIFTY2452244000PE': '39710', 'BANKNIFTY2452247500CE': '41568', 'BANKNIFTY2452247400PE': '41561', 'BANKNIFTY2452247400CE': '41558', 'BANKNIFTY2452247300PE': '41557', 'BANKNIFTY2452247300CE': '41556', 'BANKNIFTY2452255000CE': '44297', 'BANKNIFTY2452255000PE': '44299', 'BANKNIFTY2452247200PE': '41555', 'BANKNIFTY2452247200CE': '41553', 'BANKNIFTY2452247100PE': '41552', 'BANKNIFTY2452247500PE': '41569', 'BANKNIFTY2452245100PE': '39903', 'BANKNIFTY2452245200PE': '39910', 'BANKNIFTY2452245300CE': '39927', 'BANKNIFTY2452241000CE': '36449', 'BANKNIFTY2452240500PE': '36448', 'BANKNIFTY2452240500CE': '36444', 'BANKNIFTY2452256000CE': '45641', 'BANKNIFTY2452256000PE': '45642', 'BANKNIFTY2452256500CE': '45647', 'BANKNIFTY2452256500PE': '45648', 'BANKNIFTY2452257000CE': '45650', 'BANKNIFTY2452257000PE': '45691', 'BANKNIFTY2452240000PE': '36272', 'BANKNIFTY2452241000PE': '36450', 'BANKNIFTY2452240000CE': '36271', 'BANKNIFTY2452239500CE': '36269', 'BANKNIFTY2452245900PE': '40700', 'BANKNIFTY2452245900CE': '40699', 'BANKNIFTY2452245800PE': '40697', 'BANKNIFTY2452245800CE': '40691', 'BANKNIFTY2452245700PE': '40690', 'BANKNIFTY2452245700CE': '40689', 'BANKNIFTY2452245600PE': '40523', 'BANKNIFTY2452245600CE': '40520', 'BANKNIFTY2452252500CE': '41940', 'BANKNIFTY2452239500PE': '36270', 'BANKNIFTY2452241500CE': '36451', 'BANKNIFTY2452241500PE': '36454', 'BANKNIFTY2452246000CE': '41026', 'BANKNIFTY2452245300PE': '39928', 'BANKNIFTY2452245400CE': '39929', 'BANKNIFTY2452245400PE': '39930', 'BANKNIFTY2452245500CE': '39932', 'BANKNIFTY2452245500PE': '39939', 'BANKNIFTY2452246900CE': '41529', 'BANKNIFTY2452246800PE': '41164', 'BANKNIFTY2452246800CE': '41163', 'BANKNIFTY2452246700PE': '41159', 'BANKNIFTY2452246700CE': '41154', 'BANKNIFTY2452246600PE': '41137', 'BANKNIFTY2452246600CE': '41136', 'BANKNIFTY2452246500PE': '41120', 'BANKNIFTY2452246500CE': '41119', 'BANKNIFTY2452246400PE': '41081', 'BANKNIFTY2452246400CE': '41068', 'BANKNIFTY2452246300PE': '41065', 'BANKNIFTY2452246300CE': '41064', 'BANKNIFTY2452246200PE': '41043', 'BANKNIFTY2452246200CE': '41042', 'BANKNIFTY2452246100PE': '41033', 'BANKNIFTY2452246100CE': '41029', 'BANKNIFTY2452246000PE': '41028', 'BANKNIFTY2452248000PE': '41620', 'BANKNIFTY2452248100CE': '41621', 'BANKNIFTY2452245200CE': '39905', 'BANKNIFTY2452248200CE': '41639', 'BANKNIFTY2452253000CE': '42005', 'BANKNIFTY2452253000PE': '42008', 'BANKNIFTY2452253500CE': '42018', 'BANKNIFTY2452253500PE': '42019', 'BANKNIFTY2452254000CE': '42022', 'BANKNIFTY2452254000PE': '42024', 'BANKNIFTY2452254500CE': '42025', 'BANKNIFTY2452254500PE': '42026', 'BANKNIFTY2452242000CE': '37654', 'BANKNIFTY2452242000PE': '37655', 'BANKNIFTY2452251200CE': '41852', 'BANKNIFTY2452251100CE': '41850', 'BANKNIFTY2452251000CE': '41848', 'BANKNIFTY2452250900PE': '41847', 'BANKNIFTY2452250900CE': '41846', 'BANKNIFTY2452250800PE': '41839', 'BANKNIFTY2452250800CE': '41838', 'BANKNIFTY2452250700PE': '41837', 'BANKNIFTY2452250700CE': '41836', 'BANKNIFTY2452250600PE': '41831', 'BANKNIFTY2452250600CE': '41795', 'BANKNIFTY2452250500PE': '41793', 'BANKNIFTY2452251000PE': '41849', 'BANKNIFTY2452251200PE': '41853', 'BANKNIFTY2452251300CE': '41854', 'BANKNIFTY2452251300PE': '41859', 'BANKNIFTY2452252400PE': '41936', 'BANKNIFTY2452252400CE': '41934', 'BANKNIFTY2452252300PE': '41933', 'BANKNIFTY2452252300CE': '41932', 'BANKNIFTY2452252200PE': '41931', 'BANKNIFTY2452252200CE': '41930', 'BANKNIFTY2452252100PE': '41929', 'BANKNIFTY2452252100CE': '41928', 'BANKNIFTY2452252000PE': '41926', 'BANKNIFTY2452248100PE': '41638', 'BANKNIFTY2452252000CE': '41924', 'BANKNIFTY2452251900PE': '41923', 'BANKNIFTY2452251900CE': '41922', 'BANKNIFTY2452251800PE': '41919', 'BANKNIFTY2452251800CE': '41909', 'BANKNIFTY2452251700PE': '41907', 'BANKNIFTY2452251700CE': '41906', 'BANKNIFTY2452251600PE': '41900', 'BANKNIFTY2452251600CE': '41887', 'BANKNIFTY2452251500PE': '41886', 'BANKNIFTY2452251500CE': '41877', 'BANKNIFTY2452251400PE': '41874', 'BANKNIFTY2452251400CE': '41862', 'BANKNIFTY2452250500CE': '41790', 'BANKNIFTY2452250400PE': '41787', 'BANKNIFTY2452251100PE': '41851', 'BANKNIFTY2452250400CE': '41786', 'BANKNIFTY2452249300PE': '41717', 'BANKNIFTY2452249300CE': '41714', 'BANKNIFTY2452249200PE': '41712', 'BANKNIFTY2452249200CE': '41711', 'BANKNIFTY2452249100PE': '41710', 'BANKNIFTY2452249100CE': '41699', 'BANKNIFTY2452249000PE': '41698', 'BANKNIFTY2452249000CE': '41695', 'BANKNIFTY2452248900PE': '41694', 'BANKNIFTY2452248900CE': '41680', 'BANKNIFTY2452249400CE': '41718', 'BANKNIFTY2452248800PE': '41679', 'BANKNIFTY2452248700PE': '41677', 'BANKNIFTY2452248600PE': '41671', 'BANKNIFTY2452248600CE': '41667', 'BANKNIFTY2452248500PE': '41666', 'BANKNIFTY2452248500CE': '41653', 'BANKNIFTY2452248400PE': '41647', 'BANKNIFTY2452248400CE': '41646', 'BANKNIFTY2452248300PE': '41645', 'BANKNIFTY2452248300CE': '41644', 'BANKNIFTY2452248200PE': '41642', 'BANKNIFTY2452248800CE': '41678', 'BANKNIFTY2452249400PE': '41719', 'BANKNIFTY2452248700CE': '41674', 'BANKNIFTY2452249500PE': '41729', 'BANKNIFTY2452250100CE': '41760', 'BANKNIFTY2452250000PE': '41759', 'BANKNIFTY2452250000CE': '41758', 'BANKNIFTY2452249900PE': '41757', 'BANKNIFTY2452250200PE': '41780', 'BANKNIFTY2452250300CE': '41781', 'BANKNIFTY2452249500CE': '41720', 'BANKNIFTY2452250300PE': '41783', 'BANKNIFTY2452250100PE': '41761', 'BANKNIFTY2452249900CE': '41754', 'BANKNIFTY2452249800PE': '41751', 'BANKNIFTY2452250200CE': '41762', 'BANKNIFTY2452249700PE': '41749', 'BANKNIFTY2452243500PE': '38863', 'BANKNIFTY2452243500CE': '38862', 'BANKNIFTY2452249600CE': '41738', 'BANKNIFTY2452249600PE': '41739', 'BANKNIFTY2452243000PE': '38806', 'BANKNIFTY2452249800CE': '41750', 'BANKNIFTY2452243000CE': '38805', 'BANKNIFTY2452242500CE': '38420', 'BANKNIFTY2452249700CE': '41742', 'BANKNIFTY2452242500PE': '38421', 'BANKNIFTY24MAYFUT': '46923', 'BANKNIFTY24JUNFUT': '35020', 'BANKNIFTY24JULFUT': '35165'}

class Login(login_Engine):
    
    
    def __init__(self):
        pass

    def setup(self):
        
        try : 
            responce =  env.load_env_variable() if env.env_variable_initilised == False else False
            self.broker_name = env.broker_name
            session_validation_key,broker_session = self.login()
        except Exception as e :
            emergency_bot(f'Facing Issue in def login \nissue :{e}')
        
        check_validation_key = env.session_validation_key
        if session_validation_key == check_validation_key:
            return True,broker_session

        else:
            emergency_bot('Not able to Login issue in session_validation_key')
            return False, None
            

    def login(self):
        if   self.broker_name == F.kotak_neo :
            broker_session = neo_api_client.NeoAPI(consumer_key=env.consumer_key,
                               consumer_secret=env.secretKey, environment='prod',
                               access_token=None, neo_fin_key=None)
            broker_session.login(mobilenumber=env.mobileNumber, password=env.login_password)
            session = broker_session.session_2fa(env.two_factor_code)     
            return session[F.data]['greetingName'],broker_session 
            
        else : 
            emergency_bot ("Not ale to get broker_name ")
            
    
class Order_details : 
    
    def __init__(self,broker_session,broker_name):
        self.broker_session = broker_session
        self.broker_name = broker_name

    def order_book(self):
        if  self.broker_name == F.kotak_neo   :
            try :
                responce = self.broker_session.order_report()
                responce_code =  None if responce[F.stCode] == 200 else emergency_bot(f"Not able to get orderook due to : {order_staus_dict[responce[F.stCode] ]}")
                all_orders = pd.DataFrame(responce[F.data])[[F.nOrdNo,'ordDtTm','trdSym','tok',F.qty,'fldQty','avgPrc','trnsTp','prod' ,'exSeg','ordSt','stkPrc','optTp','brdLtQty','expDt','GuiOrdId']]
               
                
                all_positions = set_coloumn_name(all_orders,self.broker_name)
                all_orders = all_orders[all_orders['exchange_segement']=='nse_fo']
                filled_order = all_orders[all_orders['order_status']=='complete']
                # pending_order = all_orders[all_orders['order_status'] == 'open']
                pending_order = all_orders[all_orders['order_status'].isin(['trigger pending','open'])]
                return  all_orders,filled_order,pending_order
            except KeyError:
                print('KeyError in order_book')
                # return all_orders,filled_order,pending_order

            except Exception as e:
                emergency_bot(f'Not albe o get orderbook\nMessage : {e}')
                # return all_orders,filled_order,pending_order
            
       

    def position_book(self):
        if   self.broker_name == F.kotak_neo   :
            try :
                responce = self.broker_session.positions()
                responce_code =  None if responce[F.stCode] == 200 else emergency_bot(f"Not able to get position_book due to : {order_staus_dict[responce[F.stCode] ]}")
                all_positions = pd.DataFrame(responce[F.data]) [['trdSym','type','optTp','buyAmt' ,'prod','exSeg','tok','flBuyQty','flSellQty','sellAmt','stkPrc','expDt',]]
                all_positions = set_coloumn_name(all_positions,self.broker_name)
                option_positions = all_positions[all_positions[ F.option_type].isin([ F.CE, F.PE])]
                open_position = option_positions[option_positions['filed_buy_qty']!=option_positions['filed_sell_qty']]
                closed_position = option_positions[option_positions['filed_buy_qty']==option_positions['filed_sell_qty']]
                return all_positions,open_position,closed_position
            except KeyError:
                # Nedd to add alert
                return None,None,None

            except Exception as e:
                emergency_bot(f'Not albe o get orderbook\nMessage : {e}')
                return None,None,None


class Socket_handling:
    stocket_started = False
    future_token : str
    
    def __init__(self,broker_name,broker_session):
        self._lock = threading.Lock()  # Lock for synchronizing access
        self.broker_name= broker_name
        self.broker_session = broker_session
    
    def start_socket(self):
        df, future_token = self.prepare_option_chain_Future_token()
        with self._lock:
                    self.future_token=  future_token
        
        if self.broker_name == F.kotak_neo : 
            token_list = [{"instrument_token":i,"exchange_segment":'nse_fo'} for i in option_chain.keys()]
            
            try : 
                self.broker_session.on_message = self.update_option_chain  # called when message is received from websocket
                self.broker_session.on_error = self.on_error  # called when any error or exception occurs in code or websocket
                self.broker_session.on_open = self.on_open  # called when websocket successfully connects
                self.broker_session.on_close = self.on_close  # called when websocket connection is closed
                self.broker_session.subscribe(token_list, isIndex=False, isDepth=False)
                self.stocket_started = True
            except Exception as e:
                emergency_bot(f'Facing Issue in Socket.start \nissue : {e}')
        
    def on_error(self,message):
                emergency_bot(f'Issue in Socket : {message}')
                
    def on_open(self,message):
        logger_bot(f'Socket Started : {message}')
        
    def on_close(self,message):
        logger_bot(f'Socket Stopped : {message}')
            
 
    def update_option_chain(self, message):
        for tick in message[F.data]:
            try:
                symbol = tick['tk']
                volume =int( tick['v'])
                with self._lock:
                    option_chain[symbol]['v'] = volume
            except:
                pass

            try:
                symbol = tick['tk']
                oi = int(tick['oi'])
                with self._lock:
                    option_chain[symbol]['oi'] = oi
            except:
                pass

            try:
                symbol = tick['tk']
                ltp = float(tick['ltp'])
                with self._lock:
                    option_chain[symbol]['ltp'] = ltp
            except:
                pass
        time.sleep(1)
        # print(option_chain,'\n\n')

    def prepare_option_chain_Future_token(self):
        if self.broker_name  ==  F.kotak_neo :  
            script_master =   [i  for i in  self.broker_session.scrip_master()['filesPaths']  if 'nse_fo' in  i ]    
            df = pd.read_csv(script_master[0],low_memory=False)
            df.columns=[i.strip() for i in df.columns]
            df = df[df['pSymbolName']=='BANKNIFTY'][['pSymbol','pSymbolName','pTrdSymbol','pOptionType','pScripRefKey','lLotSize','lExpiryDate','dStrikePrice;', 'iMaxOrderSize', 'iLotSize', 'dOpenInterest']]
            df['lExpiryDate'] = df['lExpiryDate'].apply(lambda x:dt.fromtimestamp(x).date()+ relativedelta(years=10))
            df['dStrikePrice;'] = df['dStrikePrice;']/100
            df = df[['pSymbol','pScripRefKey','lExpiryDate','dStrikePrice;','pOptionType']]
            df.columns = ['instrumentToken','instrumentName','expiry','strike','optionType']
            df['days_to_expire'] = df['expiry'].apply(lambda x:(dt.strptime(str(x),'%Y-%m-%d').date()-dt.today().date()).days)
            df.sort_values('days_to_expire',inplace=True)
            df =df[(df['days_to_expire']==df['days_to_expire'].min())|(df['optionType']=='XX')]   # Find Nearest Expiry
            df.reset_index(inplace=True,drop=True)
            df.dropna(inplace = True)
            future_token =df[df['optionType']=='XX'].iloc[0]['instrumentToken']
            
            # time.sleep(2)
            with self._lock:
                for index,row in df.iterrows():
                    option_chain[row['instrumentToken']] = {'v':0,'oi':0,'ltp':0,'option_type':row['optionType'],'symbol':row['instrumentName']}
                    ticker_to_token[row['instrumentName']]=row['instrumentToken']

                logger_bot('prepared opetion chain')
                return df, future_token

def get_option_chain():
    return option_chain

def get_symbol(option_type,option_price,broker_name):
    if   broker_name == F.kotak_neo   :
        chain = pd.DataFrame(get_option_chain()).T
        chain = chain[(chain['v']>100000)&(chain['oi']>100000)]
        ce = chain[chain[ F.option_type]==option_type]
        strike = ce[ce['ltp']<=option_price].sort_values('ltp',ascending=False).iloc[0]
        return strike['symbol'],strike['ltp']

def get_ltp(instrument_token,broker_name):
    if   broker_name == F.kotak_neo   :
        ltp = option_chain[instrument_token]['ltp']
        return ltp
    
def get_token(ticker):
    return ticker_to_token[ticker]