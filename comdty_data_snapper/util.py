# -*- coding: utf-8 -*-
import blpapi

import copy
from datetime import datetime, date, timedelta
from constant import *

def get_market_from_bb_ticker (ticker):

	if (ticker.endswith("Equity")):
		return ticker[-9:-7]
	else:
		return None

def printErrorInfo(leadingStr, errorInfo):
	print("%s%s (%s)" % (leadingStr, errorInfo.getElementAsString(CATEGORY),
						 errorInfo.getElementAsString(MESSAGE)))

def parse_datetime(dt_str, format_string='%Y-%m-%d %H:%M'):
	return datetime.strptime(dt_str, format_string)