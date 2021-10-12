# -*- coding: utf-8 -*-
"""
Created on Wed Apr 21 14:57:31 2021

@author: Liqun Zhang
"""

from bloomberg import *
from datetime import date, datetime, timedelta
from constant import *

import os
import sys
import time

start = date.today() - timedelta(days = 140)
end = date.today() + timedelta(days = 1)
year = 2021 # date.today().year

root = "S:/bjin/FutMinData/Commodities/%s/"
root_save = "S:/bjin/FutMinData/Commodities/Index/"

columns = ['time', 'open', 'high', 'low', 'close', 'volume']


for i in ['SSE50 Index', 'XIN9I Index']:
	prod_name = i.split(' ')[0]
	ticker_list = [i]
	
	root_prod = root % (i)
	retry =10
	
	while retry>0:
		try:
			s = BbgSessionBar(ticker_list, ["TRADE"], 1, start, end, 'future', None, root_prod, columns)
			data = s.request_data()
			data.to_csv(root_save + prod_name + '.csv')
			
			break
		except:
			time.sleep(5)
			retry-=1
			print('Retrying : ')