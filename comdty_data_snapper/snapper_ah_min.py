from bloomberg import *

from datetime import date, datetime, timedelta
from constant import *
from config import *
from util import *
from holidays import *

import pandas as pd
from pandas.tseries.offsets import *

import os
import time

SAVE_DAILY_FILE = True
SAVE_YEARLY_FILE = True
FILE_COUNT = "5"

root = "s:\\bjin\\AH\\data\\"
daily_dir = "s:\\bjin\\AHMinData\\daily\\"
yearly_dir = "s:\\bjin\\AHMinData\\yearly\\"
cols = ['date', 'close']

dt_from = date.today() - timedelta (days = 5)
# dt_from = date(2019, 4, 26)

dt_to = date.today()
#dt_to = date(2018, 8, 6)

dt_prev = prev_weekday_as_date(date.today())

ticker_list = [

	'2600 HK Equity',
	'6116 HK Equity',
	'2318 HK Equity',
	'763 HK Equity',
	'1211 HK Equity',
	'2202 HK Equity',
	'3968 HK Equity',
	'6030 HK Equity',
	'3993 HK Equity',
	'1398 HK Equity',
	'6886 HK Equity',
	'914 HK Equity',
	'1336 HK Equity',
	'1288 HK Equity',
	'1766 HK Equity',
	'2601 HK Equity',
	'2196 HK Equity',
	'6881 HK Equity',
	'2899 HK Equity',
	'358 HK Equity',
	'1776 HK Equity',
	'2628 HK Equity',
	'3988 HK Equity',
	'1812 HK Equity',
	'3328 HK Equity',
	'939 HK Equity',
	'2611 HK Equity',
	'386 HK Equity',
	'6099 HK Equity',
	'2338 HK Equity',
	'1088 HK Equity',
	'3958 HK Equity',
	'1988 HK Equity',
	'323 HK Equity',
	'6837 HK Equity',
	'874 HK Equity',
	'2238 HK Equity',
	'3369 HK Equity',
	'1055 HK Equity',
	'2009 HK Equity',
	'2607 HK Equity',
	'1186 HK Equity',
	'6818 HK Equity',
	'2727 HK Equity',
	'2208 HK Equity',
	'390 HK Equity',
	'3606 HK Equity',
	'1513 HK Equity',
	'1065 HK Equity',
	'347 HK Equity',
	'921 HK Equity',
	'1618 HK Equity',
	'857 HK Equity',
	'1800 HK Equity',
	'1919 HK Equity',
	'753 HK Equity',
	'1108 HK Equity',
	'670 HK Equity',
	'1375 HK Equity',
	'2039 HK Equity',
	'6178 HK Equity',
	'998 HK Equity',
	'1157 HK Equity',
	'1072 HK Equity',
	'2333 HK Equity',
	'525 HK Equity',
	'1171 HK Equity',
	'338 HK Equity',
	'895 HK Equity',
	'1057 HK Equity',
	'719 HK Equity',
	'553 HK Equity',
	'168 HK Equity',
	'2880 HK Equity',
	'1898 HK Equity',
	'2866 HK Equity',
	'1138 HK Equity',
	'1033 HK Equity',
	'588 HK Equity',
	'564 HK Equity',
	'2883 HK Equity',
	'991 HK Equity',
	'1635 HK Equity',
	'902 HK Equity',
	'811 HK Equity',
	'1071 HK Equity',
	'995 HK Equity',
	'177 HK Equity',
	'317 HK Equity',
	'38 HK Equity',
	'548 HK Equity',
	'107 HK Equity',
	'42 HK Equity',
	'6806 HK Equity',
	'1658 HK Equity',
	'1339 HK Equity',
	'601600 CG Equity',
	'603157 CG Equity',
	'601318 CG Equity',
	'000063 CS Equity',
	'002594 CS Equity',
	'000002 CS Equity',
	'600036 CG Equity',
	'600030 CG Equity',
	'603993 CG Equity',
	'601398 CG Equity',
	'601688 CG Equity',
	'600585 CG Equity',
	'601336 CG Equity',
	'601288 CG Equity',
	'601766 CG Equity',
	'601601 CG Equity',
	'600196 CG Equity',
	'601881 CG Equity',
	'601899 CG Equity',
	'600362 CG Equity',
	'000776 CS Equity',
	'601628 CG Equity',
	'601988 CG Equity',
	'000488 CS Equity',
	'601328 CG Equity',
	'601939 CG Equity',
	'601211 CG Equity',
	'600028 CG Equity',
	'600999 CG Equity',
	'000338 CS Equity',
	'601088 CG Equity',
	'600958 CG Equity',
	'600016 CG Equity',
	'600808 CG Equity',
	'600837 CG Equity',
	'600332 CG Equity',
	'601238 CG Equity',
	'601326 CG Equity',
	'600029 CG Equity',
	'601992 CG Equity',
	'601607 CG Equity',
	'601186 CG Equity',
	'601818 CG Equity',
	'601727 CG Equity',
	'002202 CS Equity',
	'601390 CG Equity',
	'600660 CG Equity',
	'000513 CS Equity',
	'600874 CG Equity',
	'000898 CS Equity',
	'000921 CS Equity',
	'601618 CG Equity',
	'601857 CG Equity',
	'601800 CG Equity',
	'601919 CG Equity',
	'601111 CG Equity',
	'600876 CG Equity',
	'600115 CG Equity',
	'601375 CG Equity',
	'000039 CS Equity',
	'601788 CG Equity',
	'601998 CG Equity',
	'000157 CS Equity',
	'600875 CG Equity',
	'601633 CG Equity',
	'601333 CG Equity',
	'600188 CG Equity',
	'600688 CG Equity',
	'002672 CS Equity',
	'002703 CS Equity',
	'000756 CS Equity',
	'600775 CG Equity',
	'600600 CG Equity',
	'601880 CG Equity',
	'601898 CG Equity',
	'601866 CG Equity',
	'600026 CG Equity',
	'600871 CG Equity',
	'601588 CG Equity',
	'601717 CG Equity',
	'601808 CG Equity',
	'601991 CG Equity',
	'600635 CG Equity',
	'600011 CG Equity',
	'601811 CG Equity',
	'600027 CG Equity',
	'600012 CG Equity',
	'600377 CG Equity',
	'600685 CG Equity',
	'601038 CG Equity',
	'600548 CG Equity',
	'601107 CG Equity',
	'000585 CS Equity',
	'000166 CS Equity',
	'601658 CG Equity',
	'601319 CG Equity',
	'USDCNH Curncy',
	'USDHKD Curncy',
	'HSCEI Index', 
	'XIN9I Index',
	'HSAHP Index'
]
# ticker_list = ['HSCEI Index',
# 	'XIN9I Index',
# 	'HSAHP Index']

for t in ticker_list:

	if t.endswith("Curncy"):

		path = root + "MIN_FX_%s.hdf5" % FILE_COUNT
		fields = ["TRADE"]

	elif t.endswith("Equity"):

		path = root + "MIN_%s.hdf5" % FILE_COUNT
		fields = ["BID", "ASK", "TRADE"]

	elif t.endswith("Index") or t.endswith("Comdty"):

		path = root + "MIN_FUT_%s.hdf5" % FILE_COUNT
		fields = ["TRADE"]

	cols = ['time', 'open', 'high', 'low', 'close', 'num_events', 'volume']

	print("AH min: ", t)

	retry =5
	while retry>0:
		try:
			b = BbgSessionSingleStock(t, fields, 1, dt_from, dt_to, path, cols)
			b.request_data(save_daily_file = SAVE_DAILY_FILE, daily_dir = daily_dir, save_yearly_file = SAVE_YEARLY_FILE, yearly_dir = yearly_dir)
			break
		except:
			time.sleep(5)
			retry-=1
			print('Retrying : ')
	if retry==0:
		print("Error snapping data, resume next date")
		sys.exit(0)