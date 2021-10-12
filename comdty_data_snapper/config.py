from datetime import datetime, time
from dateutil import tz
from holidays import *
from util import *
import os 
import numpy as np

LEAD_MARKET_HOURS = { 
	
	# start, end
	
	"AU": [["T", time(7, 0, 0)], ["T", time(13, 12, 0)]],	# RMD AU vs RMD US
	#"AU": [["T", time(8, 0, 0)], ["T", time(14, 12, 0)]],	# RMD AU vs RMD US

	"HK": [["T", time(9, 30, 0)], ["T", time(15, 0, 0)]],	# 5 HK vs HSBA LN

	"US": [["T", time(21, 30, 0)], ["T+1", time(4, 0, 0)]],	# BHP US vs BHP AU
	#"US": [["T", time(22, 30, 0)], ["T+1", time(5, 0, 0)]],	# BHP US vs BHP AU

	#"LN": [["T", time(16, 0, 0)], ["T+1", time(0, 40, 0)]],	# S32 LN vs S32 AU
	"LN": [["T", time(15, 0, 0)], ["T", time(23, 40, 0)]],	# S32 LN vs S32 AU

	"SJ": [["T", time(15, 0, 0)], ["T", time(22, 49, 0)]],
}

HEDGE_MARKET_HOURS = { 
	
	# start, end
	
	"AU": [["T+1", time(7, 0, 0)], ["T+1", time(13, 12, 0)]],	# BHP US vs BHP AU
	#"AU": [["T+1", time(8, 0, 0)], ["T+1", time(14, 12, 0)]],	# BHP US vs BHP AU

	"HK": [["T+1", time(9, 30, 0)], ["T+1", time(16, 0, 0)]],	# CHL US vs 941 HK

	"US": [["T", time(21, 30, 0)], ["T+1", time(4, 0, 0)]],		# RMD AU vs RMD US
	#"US": [["T", time(22, 30, 0)], ["T+1", time(5, 0, 0)]],		# RMD AU vs RMD US
	
	#"LN": [["T", time(16, 0, 0)], ["T+1", time(0, 40, 0)]],		# 5 HK vs HSBA LN
	"LN": [["T", time(15, 0, 0)], ["T", time(23, 40, 0)]],		# 5 HK vs HSBA LN

	"SJ": [["T", time(15, 0, 0)], ["T", time(22, 49, 0)]],

}

SNAP_MARKET_HOURS = {

	# start, end

	"AU": [[ "T", time(12, 45, 0)], ["T", time(12, 50, 0)]],	# BHP US vs BHP AU
	#"AU": [[ "T", time(13, 45, 0)], ["T", time(13, 50, 0)]],	# BHP US vs BHP AU

	"HK": [[ "T", time(15, 45, 0)], ["T", time(15, 50, 0)]],	# CHL US vs 941 HK
	
	"US": [[ "T'", time(3, 45, 0)], ["T'",  time(3, 50, 0)]],		# RMD AU vs RMD US
	#"US": [[ "T", time(4, 45, 0)], ["T",  time(4, 50, 0)]],		# RMD AU vs RMD US
	
	"LN": [[ "T-1", time(23, 15, 0)], ["T-1", time(23, 20, 0)]],		# 5 HK vs HSBA LN
	#"LN": [[ "T'", time(0, 15, 0)], ["T'", time(0, 20, 0)]],		# 5 HK vs HSBA LN

	"SJ": [["T", time(22, 35, 0)], ["T", time(22, 40, 0)]],
}


PAIR_TRADING = {

	#"BHP US BBL US":
	#{
	#	"Name": "BHP US BBL US",
	#	"Root": "T:\\data\\1min.hdf5",
	#	"Columns": ['time', 'open', 'high', 'low', 'close', 'num_events', 'volume'],
	#	"Fields": ["BID", "ASK", "TRADE"],
	#	"Lead": "BHP US Equity",
	#	"Hedge": "BBL US Equity",
	#	"Pricing": [ ] ,
	#	"Format": "HDF",
	#},
	
	#"GLD US GDX US":
	#{
	#	"Name": "GLD US GDX US",
	#	"Root": "T:\\bjin\\Data\\MIN.hdf5",
	#	"Columns": ['time', 'open', 'high', 'low', 'close', 'num_events', 'volume'],
	#	"Fields": ["BID", "ASK", "TRADE"],
	#	"Lead": "GLD US Equity",
	#	"Hedge": "GDX US Equity",
	#	"Pricing": [ ] ,
	#	"Format": "HDF",
	#},	

	"MND SJ MNDI LN":
	{
		"Name": "MND SJ MNDI LN",
		"Root": "T:\\bjin\\Data\\MIN.hdf5",
		"Columns": ['time', 'open', 'high', 'low', 'close', 'num_events', 'volume'],
		"Fields": ["BID", "ASK", "TRADE"],
		"Lead": "MND SJ Equity",
		"Hedge": "MNDI LN Equity",
		"Pricing": [ ] ,
		"Format": "HDF",
	},	

}

BarArrayDType = np.dtype([
	('time', np.dtype('S16')), 
	('open', np.double), 
	('high', np.double), 
	('low', np.double), 
	('close', np.double), 
	('num_events', np.int), 
	('volume', np.int64)
])


BarDailyArrayDType = np.dtype([
	('date', np.dtype('S16')), 
	('px_last', np.double)
])


PAIRS = {

	"BHP US BHP AU": 
	{
		"Group": "US_AU",	# lead_hedge
		"Name": "BHP US BHP AU",
		"Root": "C:\mli\Research\Daily Backtest\DR",
		"Columns": ['time', 'open', 'high', 'low', 'close', 'num_events', 'volume'],
		
		"Lead": "BHP US Equity",
		"Hedge": "BHP AU Equity",
		"Pricing": [ "XP1 Index", "AUDUSD Curncy"] ,
	},

	"S32 LN S32 AU": 
	{
		"Group": "LN_AU",	# lead_hedge
		"Name": "S32 LN S32 AU",
		"Root": "C:\mli\Research\Daily Backtest\DR",
		"Columns": ['time', 'open', 'high', 'low', 'close', 'num_events', 'volume'],
		
		"Lead": "S32 LN Equity",
		"Hedge": "S32 AU Equity",
		"Pricing": [ "XP1 Index", "AUDUSD Curncy", "GBPUSD Curncy",] ,
	},

	"CYBG LN CYB AU": 
	{
		"Group": "LN_AU",	# lead_hedge
		"Name": "CYBG LN CYB AU",
		"Root": "C:\mli\Research\Daily Backtest\DR",
		"Columns": ['time', 'open', 'high', 'low', 'close', 'num_events', 'volume'],
		
		"Lead": "CYBG LN Equity",
		"Hedge": "CYB AU Equity",
		"Pricing": [ "XP1 Index", "AUDUSD Curncy", "GBPUSD Curncy",] ,
	},

	"RMD AU RMD US": 
	{
		"Group": "AU_US",	# lead_hedge
		"Name": "RMD AU RMD US",
		"Root": "C:\mli\Research\Daily Backtest\DR",
		"Columns": ['time', 'open', 'high', 'low', 'close', 'num_events', 'volume'],
		
		"Lead": "RMD AU Equity",
		"Hedge": "RMD US Equity",
		"Pricing": [ "ES1 Index", "AUDUSD Curncy"] ,
	},

	"5 HK HSBA LN": 
	{
		"Group": "HK_LN",	# lead_hedge
		"Name": "5 HK HSBA LN",
		"Root": "C:\mli\Research\Daily Backtest\DR",
		"Columns": ['time', 'open', 'high', 'low', 'close', 'num_events', 'volume'],

		"Lead": "5 HK Equity",
		"Hedge": "HSBA LN Equity",
		"Pricing": [ "Z 1 Index", "GBPUSD Curncy", "HKDUSD Curncy"] ,
	},

	"2888 HK STAN LN": 
	{
		"Group": "HK_LN",	# lead_hedge
		"Name": "2888 HK STAN LN",
		"Root": "C:\mli\Research\Daily Backtest\DR",
		"Columns": ['time', 'open', 'high', 'low', 'close', 'num_events', 'volume'],

		"Lead": "2888 HK Equity",
		"Hedge": "STAN LN Equity",
		"Pricing": [ "Z 1 Index", "GBPUSD Curncy", "HKDUSD Curncy"] ,
	},

	"805 HK GLEN LN": 
	{
		"Group": "HK_LN",	# lead_hedge
		"Name": "805 HK GLEN LN",
		"Root": "C:\mli\Research\Daily Backtest\DR",
		"Columns": ['time', 'open', 'high', 'low', 'close', 'num_events', 'volume'],

		"Lead": "805 HK Equity",
		"Hedge": "GLEN LN Equity",
		"Pricing": [ "Z 1 Index", "GBPUSD Curncy", "HKDUSD Curncy"] ,
	},

	"TCEHY US 700 HK": 
	{
		"Group": "US_HK",	# lead_hedge
		"Name": "TCEHY US 700 HK",
		"Root": "C:\mli\Research\Daily Backtest\DR",
		"Columns": ['time', 'open', 'high', 'low', 'close', 'num_events', 'volume'],

		"Lead": "TCEHY US Equity",
		"Hedge": "700 HK Equity",
		"Pricing": [ "HC1 Index", "HKDUSD Curncy"] ,
	},
	
	"WBK US WBC AU": 
	{
		"Group": "US_AU",	# lead_hedge
		"Name": "WBK US WBC AU",
		"Root": "C:\mli\Research\Daily Backtest\DR",
		"Columns": ['time', 'open', 'high', 'low', 'close', 'num_events', 'volume'],
		
		"Lead": "WBK US Equity",
		"Hedge": "WBC AU Equity",
		"Pricing": [ "XP1 Index", "AUDUSD Curncy"] ,
	},
}


class stock_pair_snapper (object):

	def __init__ (self, cfg, trade_date):

		self.trade_date = trade_date
		self.name = cfg['Name']
		self.lead_name = cfg["Lead"]
		self.lead_market  = self._get_lead_market()
		self.lead_bar_time = self._get_lead_bar_time(trade_date)

		self.hedge_name = cfg["Hedge"]
		self.hedge_market  = self._get_hedge_market()
		self.hedge_bar_time = self._get_hedge_bar_time(trade_date)

		self.pricing_names = cfg["Pricing"]

		self.fields = cfg["Fields"]
		self.root = cfg["Root"]
		#self.path = os.path.join(self.root, trade_date.strftime("%Y%m%d"))
		self.cols = cfg["Columns"]
		self.format = cfg["Format"]

		self.bars = {
			self.lead_name: self.lead_bar_time,
			self.hedge_name: self.hedge_bar_time,
			}
		for n in self.pricing_names:
			self.bars[n] = [self.lead_bar_time[0], self.hedge_bar_time[1]]

		#if not os.path.exists(self.root):
		#	os.makedirs(self.root)

	def _get_lead_market(self):
		return get_market_from_bb_ticker(self.lead_name)

	def _get_hedge_market(self):
		return get_market_from_bb_ticker(self.hedge_name)

	def _get_lead_bar_time (self, td):

		session_start = LEAD_MARKET_HOURS[self.lead_market][0]
		session_end = LEAD_MARKET_HOURS[self.lead_market][1]
		
		time_start = self._get_timestamp_from_session(self.lead_market, td, session_start)
		time_end = self._get_timestamp_from_session(self.lead_market, td, session_end)

		return [time_start, time_end]


	def _get_hedge_bar_time (self, td):

		session_start = HEDGE_MARKET_HOURS[self.hedge_market][0]
		session_end = HEDGE_MARKET_HOURS[self.hedge_market][1]		
		time_start = self._get_timestamp_from_session(self.hedge_market, td, session_start)
		time_end = self._get_timestamp_from_session(self.hedge_market, td, session_end)
		return [time_start, time_end]

	def _get_timestamp_from_session (self, market, td, session):

		''' 
		e.g. market = HK, td = date(2017, 3, 9), session = ["T+1", time(7, 0, 0)]
		'''

		d = session[0]
		t = session[1]

		td_next_date = next_weekday_as_date(td, market)
		td_prev_date = prev_weekday_as_date(td, market)

		if d == "T":
			return self._hk_to_utc_time(datetime.combine(td, t))

		elif d == "T'":
			if td.weekday() == 0:
				td = td - timedelta(days = 2)
			return  self._hk_to_utc_time(datetime.combine(td, t))

		elif d == "T+1":
			return self._hk_to_utc_time(datetime.combine(td_next_date, t))

		elif d == "T-1":
			return self._hk_to_utc_time(datetime.combine(td_prev_date, t))

	def _hk_to_utc_time (self, hk_time):
		
		from_zone = tz.gettz("Asia/Hong_Kong")
		to_zone = tz.gettz('UTC')
		hk = hk_time.replace(tzinfo = from_zone)
		utc = hk.astimezone(to_zone)
		result = utc.replace (tzinfo = None)
		return result



class dr_pair_snapper (object):
	
	def __init__ (self, cfg, trade_date):

		self.trade_date = trade_date
		self.group = cfg["Group"]
		self.name = cfg["Name"]

		self.lead_name  = cfg["Lead"]
		self.lead_market  = self._get_lead_market()
		self.lead_bar_time = self._get_lead_bar_time(trade_date)
		
		self.hedge_name  = cfg["Hedge"]
		self.hedge_market  = self._get_hedge_market()
		self.hedge_bar_time = self._get_hedge_bar_time(trade_date)
		
		self.price_names = cfg["Pricing"]
		self.cols = cfg["Columns"]

		self.root = cfg["Root"]
		self.path_common = os.path.join(self.root, self.group, "Common", trade_date.strftime("%Y%m%d"))
		self.path_daily = os.path.join(self.root, self.group, self.name, trade_date.strftime("%Y%m%d"))

		if not os.path.exists(self.path_common):
			os.makedirs(self.path_common)
		if not os.path.exists(self.path_daily):
			os.makedirs(self.path_daily)

		self.bars = {
			self.lead_name: self.lead_bar_time,
			self.hedge_name: self.hedge_bar_time,
			}

		for n in self.price_names:
			self.bars[n] = [self.lead_bar_time[0], self.hedge_bar_time[1]]

		self.snap_names = [self.hedge_name] + self.price_names
		self.snap_market = self._get_snap_market()
		self.snap_bar_time = self._get_snap_bar_time(trade_date)
		self.snap_path = os.path.join(self.path_daily, "snap.csv")

		self.is_holiday = is_holiday(trade_date, self.lead_market)


	def _get_path (self, symbol):
		
		if symbol in [self.lead_name, self.hedge_name]:
			return self.path_daily
		else:
			return self.path_common

	
	def _get_lead_market(self):
		return get_market_from_bb_ticker(self.lead_name)


	def _get_hedge_market(self):
		return get_market_from_bb_ticker(self.hedge_name)


	def _get_snap_market(self):
		return self._get_hedge_market()

		
	def _get_lead_bar_time (self, td):

		session_start = LEAD_MARKET_HOURS[self.lead_market][0]
		session_end = LEAD_MARKET_HOURS[self.lead_market][1]
		
		time_start = self._get_timestamp_from_session(self.lead_market, td, session_start)
		time_end = self._get_timestamp_from_session(self.lead_market, td, session_end)

		return [time_start, time_end]


	def _get_hedge_bar_time (self, td):

		session_start = HEDGE_MARKET_HOURS[self.hedge_market][0]
		session_end = HEDGE_MARKET_HOURS[self.hedge_market][1]
		
		time_start = self._get_timestamp_from_session(self.hedge_market, td, session_start)
		time_end = self._get_timestamp_from_session(self.hedge_market, td, session_end)

		return [time_start, time_end]


	def _get_snap_bar_time(self, td):

		session_start = SNAP_MARKET_HOURS[self.snap_market][0]
		session_end = SNAP_MARKET_HOURS[self.snap_market][1]
		
		time_start = self._get_timestamp_from_session(self.snap_market, td, session_start)
		time_end = self._get_timestamp_from_session(self.snap_market, td, session_end)

		return [time_start, time_end]

	
	def _get_timestamp_from_session (self, market, td, session):

		''' 
		e.g. market = HK, td = date(2017, 3, 9), session = ["T+1", time(7, 0, 0)]
		'''

		d = session[0]
		t = session[1]

		td_next_date = next_weekday_as_date(td, market)
		td_prev_date = prev_weekday_as_date(td, market)

		if d == "T":
			return self._hk_to_utc_time(datetime.combine(td, t))

		elif d == "T'":
			if td.weekday() == 0:
				td = td - timedelta(days = 2)
			return  self._hk_to_utc_time(datetime.combine(td, t))

		elif d == "T+1":
			return self._hk_to_utc_time(datetime.combine(td_next_date, t))

		elif d == "T-1":
			return self._hk_to_utc_time(datetime.combine(td_prev_date, t))

	def _hk_to_utc_time (self, hk_time):
		
		from_zone = tz.gettz("Asia/Hong_Kong")
		to_zone = tz.gettz('UTC')
		hk = hk_time.replace(tzinfo = from_zone)
		utc = hk.astimezone(to_zone)
		result = utc.replace (tzinfo = None)
		return result