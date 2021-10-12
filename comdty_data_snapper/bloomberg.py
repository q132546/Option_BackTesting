import sys
import blpapi
import pandas as pd
import os
import h5py
import bisect
import numpy as np

from holidays import *
from datetime import datetime
from dateutil import tz
from config import *
from util import *
from constant import *

class BbgSession(object):
	
	def __init__(self):
		pass
		
	def start_session(self):

		sessionOptions = blpapi.SessionOptions()
		sessionOptions.setServerHost('localhost')
		sessionOptions.setServerPort(8194)
		self.session = blpapi.Session(sessionOptions)
		if not self.session.start():
			print("Failed to start session.")
			return

	def stop_session(self):
		self.session.stop()

	def open_service (self):
		pass

	def request_data (self):
		pass

	def _utc_to_hk_time (self, utc_time):
		
		from_zone = tz.gettz('UTC')
		to_zone = tz.gettz("Asia/Hong_Kong")
		utc = utc_time.replace(tzinfo = from_zone)
		local = utc.astimezone(to_zone)
		result = local.replace (tzinfo = None)

		return result

	def _hk_to_utc_time (self, hk_time):
		
		from_zone = tz.gettz("Asia/Hong_Kong")
		to_zone = tz.gettz('UTC')
		hk = hk_time.replace(tzinfo = from_zone)
		utc = hk.astimezone(to_zone)
		result = utc.replace (tzinfo = None)
		return result

class BbgSessionTimeStamp (BbgSession):

	def __init__(self, ticker_list, field_list, interval, start, end, file_name):

		self.ticker_list = ticker_list
		self.field_list = field_list
		self.interval = interval
		self.start = start
		self.end = end
		self.file_name = file_name
		self.data = {}
		self.bar_list = {}
		self.start_session()
		self.open_service()
	
	def open_service (self):
		
		if not self.session.openService("//blp/refdata"):
			print("Failed to open //blp/refdata")
			return

		self.refDataService = self.session.getService("//blp/refdata")
	
	def request_data(self):
		
		cols = ["Symbol", "Bid", "Ask"]
		data_list = []
		
		for t in self.ticker_list:
			try:
				row = [t]

				for f in self.field_list:
				
					print("Snaping data %s %s data..." % ( t, f ))
				
					request = self.refDataService.createRequest("IntradayBarRequest")
					request.set("security", t)
					request.set("eventType", f)
					request.set("interval", self.interval)
					request.set("startDateTime", self.start)
					request.set("endDateTime", self.end)
				
					self.session.sendRequest(request)

					while(True):
						ev = self.session.nextEvent()

						for msg in ev:
							
							if msg.hasElement(RESPONSE_ERROR):
								printErrorInfo("REQUEST FAILED: ", msg.getElement(RESPONSE_ERROR))
								continue
							if msg.messageType() in ["SessionConnectionUp", "SessionStarted", "ServiceOpened"] : 
								# log
								pass
							else:
								data = msg.getElement(BAR_DATA).getElement(BAR_TICK_DATA)
								for d in data.values():
									row.append(d.getElementAsFloat(CLOSE))

						if ev.eventType() == blpapi.Event.RESPONSE:
							break

				data_list.append(tuple(row))

			except:
				e = sys.exc_info()[0]
				print(e)
				continue
		
		data_df = pd.DataFrame.from_records(data = data_list, columns = cols, index = "Symbol")
		data_df.to_csv(self.file_name)
		
		return
		
class BbgSessionTick(BbgSession):

	def __init__(self, ticker_list, field_list, interval, start, end, product, output_method, file_root, cols, dt=None):

		self.ticker_list = ticker_list
		self.field_list = field_list
		self.interval = interval
		self.start = start
		self.end = end
		self.product = product
		self.output_method = output_method
		self.file_root = file_root
		self.cols = cols
		self.data = {}
		self.bar_list = {}
		self.start_session()
		self.open_service()
		self.dt = dt
		
	def open_service (self):
		
		if not self.session.openService("//blp/refdata"):
			print("Failed to open //blp/refdata")
			return

		self.refDataService = self.session.getService("//blp/refdata")

	def request_data (self):
		#labels = ['time', 'open', 'high', 'low', 'close', 'num_events', 'volume']
		#labels = ['time', 'close']
		
		data_all = pd.DataFrame()
		for t in self.ticker_list:
			for f in self.field_list:
				data_list = []
				print("Requesting %s %s data..." % ( t, f ))

				request = self.refDataService.createRequest("IntradayTickRequest")
				request.set("security", t)

				request.set("startDateTime", self.start)
				request.set("endDateTime", self.end)
				
				# Add fields to request
				
				eventTypes = request.getElement("eventTypes")
				eventTypes.appendValue("TRADE")
				eventTypes.appendValue("BID")
				eventTypes.appendValue("ASK")
								
				self.session.sendRequest(request)
				ev = self.session.nextEvent()
				print(ev)
				try:
					while(True):
						ev = self.session.nextEvent()

						for msg in ev:
							
							if msg.hasElement(RESPONSE_ERROR):
								printErrorInfo("REQUEST FAILED: ", msg.getElement(RESPONSE_ERROR))
								continue
							if msg.messageType() in ["SessionConnectionUp", "SessionStarted", "ServiceOpened"] : 
								# log
								pass
							else:
								data = msg.getElement(BAR_DATA).getElement(BAR_TICK_DATA)
								for d in data.values():
									
									s = self._get_row_content(d)
									data_list.append(s)

						if ev.eventType() == blpapi.Event.RESPONSE:
							break

					data_df = pd.DataFrame.from_records(data = data_list, columns = self.cols, index = "time")
					
					print(data_df)
			
				except:
					pass

class BbgSessionBar(BbgSession):

	def __init__(self, ticker_list, field_list, interval, start, end, product, output_method, file_root, cols, dt=None):

		self.ticker_list = ticker_list
		self.field_list = field_list
		self.interval = interval
		self.start = start
		self.end = end
		self.product = product
		self.output_method = output_method
		self.file_root = file_root
		self.cols = cols
		self.data = {}
		self.bar_list = {}
		self.start_session()
		self.open_service()
		self.dt = dt
		
	def open_service (self):
		
		if not self.session.openService("//blp/refdata"):
			print("Failed to open //blp/refdata")
			return

		self.refDataService = self.session.getService("//blp/refdata")

	def request_data (self):
		#labels = ['time', 'open', 'high', 'low', 'close', 'num_events', 'volume']
		#labels = ['time', 'close']
		
		data_all = pd.DataFrame()
		for t in self.ticker_list:
			for f in self.field_list:
				data_list = []
				print("Requesting %s %s data..." % ( t, f ))

				request = self.refDataService.createRequest("IntradayBarRequest")
				request.set("security", t)
				request.set("eventType", f)
				request.set("interval", self.interval)
				request.set("startDateTime", self.start)
				request.set("endDateTime", self.end)
				
				self.session.sendRequest(request)
				try:
					while(True):
						ev = self.session.nextEvent()

						for msg in ev:
							
							if msg.hasElement(RESPONSE_ERROR):
								printErrorInfo("REQUEST FAILED: ", msg.getElement(RESPONSE_ERROR))
								continue
							if msg.messageType() in ["SessionConnectionUp", "SessionStarted", "ServiceOpened"] : 
								# log
								pass
							else:
								data = msg.getElement(BAR_DATA).getElement(BAR_TICK_DATA)
								for d in data.values():
									
									s = self._get_row_content(d)
									data_list.append(s)

						if ev.eventType() == blpapi.Event.RESPONSE:
							break

					data_df = pd.DataFrame.from_records(data = data_list, columns = self.cols, index = "time")


					if self.product == 'future':
						if t[:2] in ['S ', 'Z ']:
							t = t[:4]
						else:
							t = t.split(' ')[0]
						
						if t[-1] == '9':
							data_df['year'] = '2019'
						elif t[-1] == '0':
							data_df['year'] = '2020'
						elif t[-1] == '1':
							data_df['year'] = '2021'
						elif t[-1] == '2':
							data_df['year'] = '2022'
						
						data_df['contract'] = t
						data_all = pd.concat([data_all, data_df])
						
					elif self.product == 'spot':
						columns = [t]
						data_df.columns = columns
						data_df.index = pd.to_datetime(data_df.index)

					elif self.product == 'dr':
						data_df.index = pd.to_datetime(data_df.index)

					if self.output_method == "file":
						file_name = "%s.csv" % (t)

						data = pd.read_csv(os.path.join(self.file_root, file_name), index_col = 0)
						data.index = pd.to_datetime(data.index)
						data = pd.concat([data, data_df])
						data = data.loc[~data.index.duplicated(keep = 'first')]
						data.to_csv(os.path.join(self.file_root, file_name))
					
					elif self.output_method == "filename":
						data_df = pd.DataFrame.from_records(data = data_list, columns = self.cols, index = "time")
						data_df.to_csv(self.file_root)

					elif self.output_method == 'HDF':

						if len(data_list) > 0:
							if os.path.exists(self.file_root):
								hf = h5py.File(self.file_root, 'a')
							else:
								hf = h5py.File(self.file_root, 'w')
							dat = [(tup[0].strftime("%Y-%m-%d %H:%M:%S"), tup[1], tup[2], tup[3], tup[4], tup[5], tup[6]) for tup in data_list]
							#dat = [(np.datetime64(tup[0]), tup[1], tup[2], tup[3], tup[4], tup[5], tup[6]) for tup in data_list]
							data_array = np.array(dat, BarArrayDType),
							ds_name = '/%s/%s/%s' % (t, f, self.dt.strftime("%Y%m%d"))
							if ds_name in hf:
								del hf[ds_name]
							data_set = hf.create_dataset(ds_name, data = data_array[0])
							
							hf.flush()
							hf.close()

				except:
					e = sys.exc_info()[0]
					print(e)
					continue;
		return data_all

	def _get_row_content (self, d):
		
		row = []
		
		for c in self.cols:
			c_name = COL_MAP[c]
			if c_name == TIME:
				row.append(self._utc_to_hk_time(d.getElementAsDatetime(c_name)))

			elif c_name in [NUM_EVENTS, VOLUME]:
				row.append(d.getElementAsInteger(c_name))

			else:		# [OPEN, HIGH, LOW, CLOSE]
				row.append(d.getElementAsFloat(c_name))

		return tuple(row)

class BbgSessionHist(BbgSession):

	def __init__(self, ticker_list, field_list, start, end, cols):

		self.ticker_list = ticker_list
		self.field_list = field_list
		
		self.start = start
		self.end = end
		self.cols = cols

		self.data = {}
		self.bar_list = {}
		
		self.start_session()
		self.open_service()

		
	def open_service (self):
		
		if not self.session.openService("//blp/refdata"):
			print ("Failed to open //blp/refdata")
			return

		self.refDataService = self.session.getService("//blp/refdata")

	def request_data (self):
		
		
		#labels = ['time', 'open', 'high', 'low', 'close', 'num_events', 'volume']
		#labels = ['time', 'close']
		
		request = self.refDataService.createRequest("HistoricalDataRequest")

		for t in self.ticker_list:

			request.getElement("securities").appendValue(t)

		for f in self.field_list:

			request.getElement("fields").appendValue(f)
				
		request.set("periodicitySelection", "DAILY")
		request.set("startDate", self.start)
		request.set("endDate", self.end)
		#request.set("maxDataPoints", 100)
		
		self.session.sendRequest(request)
		
		try:
			while(True):
				ev = self.session.nextEvent()
				#print(ev)

				for msg in ev:
					
					data_list = []

					if msg.hasElement(RESPONSE_ERROR):
						printErrorInfo("REQUEST FAILED: ", msg.getElement(RESPONSE_ERROR))
						continue
						
					if msg.messageType() in ["SessionConnectionUp", "SessionStarted", "ServiceOpened"] : 
						# log
						pass
						
					else:
						sec = msg.getElement(SECURITY_DATA).getElement(SECURITY_NAME)
						sec_name = list(sec.values())[0]
						data = msg.getElement(SECURITY_DATA).getElement(FIELD_DATA)
						
						for d in data.values():
							s = self._get_row_content(d)
							data_list.append(s)
						#print(data_list)

						
						df = pd.DataFrame.from_records(data = data_list, columns = ['date', 'PX_LAST', 'PX_OPEN'])
												
							
						#sec = msg.getElement(SECURITY_DATA).getElement(SECURITY_NAME)
						#sec_name = list(sec.values())[0]
						#data = msg.getElement(SECURITY_DATA).getElement(FIELD_DATA)
						
						#for d in data.values():
									
							#s = self._get_row_content(d)
							#data_list.append(s)

						
					
					#print(self.data)

				if ev.eventType() == blpapi.Event.RESPONSE:
					break
		except:
			e = sys.exc_info()[0]
			print (e)
		print(df)
		return df

	def _get_row_content (self, d):
		
		row = []
		
		for c in ['date', 'PX_LAST', 'PX_OPEN']:
			
			if c == "date":
				row.append(d.getElementAsDatetime(c))

			else:		# [OPEN, HIGH, LOW, CLOSE]

				row.append(d.getElementAsFloat(c))

		return tuple(row)

class BbgSessionSingleStock(BbgSession):

	def __init__(self, ticker, field_list, interval, start, end, file_root, cols):

		self.ticker = ticker
		self.field_list = field_list
		self.interval = interval
		self.start = start
		self.end = end
		self.file_root = file_root
		self.cols = cols
		self.data = {}
		#self.bar_list = {}
		self.start_session()
		self.open_service()
		
	def open_service (self):
		
		if not self.session.openService("//blp/refdata"):
			print ("Failed to open //blp/refdata")
			return

		self.refDataService = self.session.getService("//blp/refdata")

	def request_data (self, save_daily_file = False, daily_dir = None, save_yearly_file = False, yearly_dir = None):

		if save_daily_file:	# bjin: to create daily hdf 
			prev_dt_str = prev_weekday_as_string(self.end)
			prev_dt = prev_weekday_as_date(self.end)
			root_daily = os.path.join(daily_dir, prev_dt_str + ".hdf5")

		for f in self.field_list:
			data_list = []
			print ("Requesting %s %s data..." % ( self.ticker, f ))
				
			request = self.refDataService.createRequest("IntradayBarRequest")
			request.set("security", self.ticker)
			request.set("eventType", f)
			request.set("interval", self.interval)
			request.set("startDateTime", self.start)
			request.set("endDateTime", self.end)
				
			self.session.sendRequest(request)
			try:
				while(True):
					ev = self.session.nextEvent()

					for msg in ev:
							
						if msg.hasElement(RESPONSE_ERROR):
							printErrorInfo("REQUEST FAILED: ", msg.getElement(RESPONSE_ERROR))
							continue
						if msg.messageType() in ["SessionConnectionUp", "SessionStarted", "ServiceOpened"] : 
							# log
							pass
						else:
							data = msg.getElement(BAR_DATA).getElement(BAR_TICK_DATA)
							for d in data.values():
									
								s = self._get_row_content(d)
								data_list.append(s)

					if ev.eventType() == blpapi.Event.RESPONSE:
						break

				# add logic to handle file append ( download data 2nd time )
				if len(data_list) > 0:
					
					# <<1 - save consolidated hdf >>

					hf = h5py.File(self.file_root, 'a')
					ds_name = '/%s/%s' % (self.ticker, f)
					t_format = "%Y-%m-%d %H:%M"

					if ds_name in hf:	# update existing dataset

						dat_arr = hf[ds_name][()]		# read existing data as array
						end_time = datetime.strptime(dat_arr[-1][0], t_format)

						start_time_new = data_list[0][0]
						end_time_new = data_list[-1][0]
						
						if start_time_new > end_time:	# insert everthing new
							
							dat_new = [(tup[0].strftime(t_format), tup[1], tup[2], tup[3], tup[4], tup[5], tup[6]) for tup in data_list]
							dat_arr_new = np.array(dat_new, BarArrayDType)
							if dat_arr.dtype != BarArrayDType:
								dat_arr = dat_arr.astype(BarArrayDType)
							arr = np.concatenate((dat_arr, dat_arr_new))

						elif start_time_new < end_time and end_time < end_time_new:	# insert partially

							time_index = [a[0] for a in data_list]
							if end_time in time_index: 
								data_list = data_list[time_index.index(end_time)+1:]
							else:
								data_list = data_list[bisect.bisect_left(time_index, end_time) : ]
							dat_new = [(tup[0].strftime(t_format), tup[1], tup[2], tup[3], tup[4], tup[5], tup[6]) for tup in data_list]
							dat_arr_new = np.array(dat_new, BarArrayDType)
							if dat_arr.dtype != BarArrayDType:
								dat_arr = dat_arr.astype(BarArrayDType)
							arr = np.concatenate((dat_arr, dat_arr_new))
						else:		# end_time > end_time_now, nothing to insert
							arr = dat_arr

						# reinsert data
						del hf[ds_name]
						data_set = hf.create_dataset(ds_name, data = arr)

					else:	# completely new dataset
						
						dat = [(tup[0].strftime(t_format), tup[1], tup[2], tup[3], tup[4], tup[5], tup[6]) for tup in data_list]
						data_array = np.array(dat, BarArrayDType),
						data_set = hf.create_dataset(ds_name, data = data_array[0])
							
					hf.flush()
					hf.close()

					# << end of 1 >>

					# << 2 save daily hdf >>
					hf_daily = h5py.File(root_daily, 'a')
					if ds_name in hf_daily:
						del hf_daily[ds_name] # delete old data
					dat_daily = [(tup[0].strftime(t_format), tup[1], tup[2], tup[3], tup[4], tup[5], tup[6]) for tup in data_list if tup[0].date() == prev_dt]
					# dat_daily = [(tup[0].strftime(t_format), tup[1], tup[2], tup[3], tup[4], tup[5], tup[6]) for tup in data_list]
					data_array_daily = np.array(dat_daily, BarArrayDType),
					data_set_daily = hf_daily.create_dataset(ds_name, data = data_array_daily[0])
					hf_daily.flush()
					hf_daily.close()

					# << 3 save yearly hdf >>

					if save_yearly_file and f == "TRADE":

						year_str = str(prev_weekday_as_date(date.today()).year)
						root_yearly = os.path.join(yearly_dir, year_str + ".hdf5")
						
						if os.path.exists(root_yearly):
							hf_yearly = h5py.File(root_yearly, 'a')
							if ds_name in hf_yearly:
								dat_arr_yr = hf_yearly[ds_name][()]

								if dat_arr_yr.dtype != BarArrayDType:
									dat_arr_yr = dat_arr_yr.astype(BarArrayDType)
								
								arr_yr = np.concatenate((dat_arr_yr, data_array_daily[0]))
								del hf_yearly[ds_name]
								data_set = hf_yearly.create_dataset(ds_name, data = arr_yr)
							else:
								hf_yearly = h5py.File(root_yearly, 'a')
								data_set = hf_yearly.create_dataset(ds_name, data=data_array_daily[0])


						else:	# new year - create a new file here 
							hf_yearly = h5py.File(root_yearly, 'a')
							data_set = hf_yearly.create_dataset(ds_name, data = data_array_daily[0])

						hf_yearly.flush()
						hf_yearly.close()
					# << end of yearly hdf >> 
					
			except:
				e = sys.exc_info()
				print (e)
				continue;
		return data_list

	def _get_row_content (self, d):
		
		row = []
		
		for c in self.cols:
			c_name = COL_MAP[c]
			if c_name == TIME:
				row.append(self._utc_to_hk_time(d.getElementAsDatetime(c_name)))

			elif c_name in [NUM_EVENTS, VOLUME]:
				row.append(d.getElementAsInteger(c_name))

			else:		# [OPEN, HIGH, LOW, CLOSE]
				row.append(d.getElementAsFloat(c_name))

		return tuple(row)

class BbgSessionSingleStockHist(BbgSession):

	def __init__(self, ticker_list, start, end, cols, file_root):

		self.ticker_list = ticker_list
		self.field = "PX_LAST"
		
		self.start = start
		self.end = end
		self.cols = cols
		self.file_root = file_root

		self.data = {}
		self.bar_list = {}
		
		self.start_session()
		self.open_service()

		
	def open_service (self):
		
		if not self.session.openService("//blp/refdata"):
			print ("Failed to open //blp/refdata")
			return

		self.refDataService = self.session.getService("//blp/refdata")

	def request_data (self):
		
		request = self.refDataService.createRequest("HistoricalDataRequest")

		for t in self.ticker_list:

			request.getElement("securities").appendValue(t)

		request.getElement("fields").appendValue(self.field)
				
		request.set("periodicityAdjustment", "ACTUAL")
		request.set("periodicitySelection", "DAILY")
		request.set("startDate", self.start.strftime("%Y%m%d"))
		request.set("endDate", self.end.strftime("%Y%m%d"))
		
		self.session.sendRequest(request)
		try:
			while(True):
				ev = self.session.nextEvent()

				for msg in ev:
					
					data_list = []

					if msg.hasElement(RESPONSE_ERROR):
						printErrorInfo("REQUEST FAILED: ", msg.getElement(RESPONSE_ERROR))
						continue
					if msg.messageType() in ["SessionConnectionUp", "SessionStarted", "ServiceOpened"] : 
						# log
						pass
					else:
						sec = msg.getElement(SECURITY_DATA).getElement(SECURITY_NAME)
						sec_name = list(sec.values())[0]
						data = msg.getElement(SECURITY_DATA).getElement(FIELD_DATA)
						for d in data.values():
									
							s = self._get_row_content(d)
							data_list.append(s)

						if len(data_list) > 0 :

							hf = h5py.File(self.file_root, 'a')
							ds_name = '/%s' % (sec_name)
							t_format = "%Y-%m-%d %H:%M"

							if ds_name in hf:	# append to existing dataset

								dat_arr = hf[ds_name][()]		# read existing data as array
								end_time = datetime.strptime(dat_arr[-1][0], t_format)
								start_time_new = datetime.combine(data_list[0][0], time(0,0))
								end_time_new = datetime.combine(data_list[-1][0], time(0,0))

								if start_time_new > end_time:	# insert everthing new
							
									dat_new = [(tup[0].strftime(t_format), tup[1]) for tup in data_list]
									dat_arr_new = np.array(dat_new, BarDailyArrayDType)
									arr = np.concatenate((dat_arr, dat_arr_new))

								elif start_time_new < end_time and end_time < end_time_new:	# insert partially

									time_index = [a[0] for a in data_list]
									data_list = data_list[time_index.index(end_time.date())+1:]
									dat_new = [(tup[0].strftime(t_format), tup[1]) for tup in data_list]
									dat_arr_new = np.array(dat_new, BarDailyArrayDType)
									arr = np.concatenate((dat_arr, dat_arr_new))

								else:		# end_time > end_time_now, nothing to insert
									arr = dat_arr

								del hf[ds_name]		# reinsert data
								data_set = hf.create_dataset(ds_name, data = arr)

							else:	# store new dataset
								dat = [(tup[0].strftime(t_format), tup[1]) for tup in data_list]
								data_array = np.array(dat, BarDailyArrayDType),
								data_set = hf.create_dataset(ds_name, data = data_array[0])
							hf.flush()
							hf.close()

				if ev.eventType() == blpapi.Event.RESPONSE:
					break
		except:
			e = sys.exc_info()[0]
			print (e)
			
		return

	def _get_row_content (self, d):
		
		row = []
		
		for c in self.cols:
			
			if c == "date":
				row.append(d.getElementAsDatetime(c))

			elif c == 'close':
				row.append(d.getElementAsFloat("PX_LAST"))

		return tuple(row)

class BbgSessionSingleStockHistVWAP(BbgSession):

	def __init__(self, ticker_list, start, end, cols, file_root):

		self.ticker_list = ticker_list
		self.field = "EQY_WEIGHTED_AVG_PX"
		
		self.start = start
		self.end = end
		self.cols = cols
		self.file_root = file_root

		self.data = {}
		self.bar_list = {}
		
		self.start_session()
		self.open_service()

		
	def open_service (self):
		
		if not self.session.openService("//blp/refdata"):
			print ("Failed to open //blp/refdata")
			return

		self.refDataService = self.session.getService("//blp/refdata")

	def request_data (self):
		
		request = self.refDataService.createRequest("HistoricalDataRequest")

		for t in self.ticker_list:

			request.getElement("securities").appendValue(t)

		request.getElement("fields").appendValue(self.field)
				
		request.set("periodicityAdjustment", "ACTUAL")
		request.set("periodicitySelection", "DAILY")
		request.set("startDate", self.start.strftime("%Y%m%d"))
		request.set("endDate", self.end.strftime("%Y%m%d"))
		
		self.session.sendRequest(request)
		try:
			while(True):
				ev = self.session.nextEvent()

				for msg in ev:
					
					data_list = []

					if msg.hasElement(RESPONSE_ERROR):
						printErrorInfo("REQUEST FAILED: ", msg.getElement(RESPONSE_ERROR))
						continue
					if msg.messageType() in ["SessionConnectionUp", "SessionStarted", "ServiceOpened"] : 
						# log
						pass
					else:
						sec = msg.getElement(SECURITY_DATA).getElement(SECURITY_NAME)
						sec_name = list(sec.values())[0]
						data = msg.getElement(SECURITY_DATA).getElement(FIELD_DATA)
						for d in data.values():
									
							s = self._get_row_content(d)
							data_list.append(s)

						if len(data_list) > 0 :

							hf = h5py.File(self.file_root, 'a')
							ds_name = '/%s' % (sec_name)
							t_format = "%Y-%m-%d %H:%M"

							if ds_name in hf:	# append to existing dataset

								dat_arr = hf[ds_name][()]		# read existing data as array
								end_time = datetime.strptime(dat_arr[-1][0], t_format)
								start_time_new = datetime.combine(data_list[0][0], time(0,0))
								end_time_new = datetime.combine(data_list[-1][0], time(0,0))

								if start_time_new > end_time:	# insert everthing new
							
									dat_new = [(tup[0].strftime(t_format), tup[1]) for tup in data_list]
									dat_arr_new = np.array(dat_new, BarDailyArrayDType)
									arr = np.concatenate((dat_arr, dat_arr_new))

								elif start_time_new < end_time and end_time < end_time_new:	# insert partially

									time_index = [a[0] for a in data_list]
									data_list = data_list[time_index.index(end_time.date())+1:]
									dat_new = [(tup[0].strftime(t_format), tup[1]) for tup in data_list]
									dat_arr_new = np.array(dat_new, BarDailyArrayDType)
									arr = np.concatenate((dat_arr, dat_arr_new))

								else:		# end_time > end_time_now, nothing to insert
									arr = dat_arr

								del hf[ds_name]		# reinsert data
								data_set = hf.create_dataset(ds_name, data = arr)

							else:	# store new dataset
								dat = [(tup[0].strftime(t_format), tup[1]) for tup in data_list]
								data_array = np.array(dat, BarDailyArrayDType),
								data_set = hf.create_dataset(ds_name, data = data_array[0])
							hf.flush()
							hf.close()

				if ev.eventType() == blpapi.Event.RESPONSE:
					break
		except:
			e = sys.exc_info()[0]
			print (e)
			
		return

	def _get_row_content (self, d):
		
		row = []
		
		for c in self.cols:
			
			if c == "date":
				row.append(d.getElementAsDatetime(c))

			elif c == 'close':
				row.append(d.getElementAsFloat("EQY_WEIGHTED_AVG_PX"))

		return tuple(row)

class BbgSessionIndexWeight (BbgSession):

	def __init__(self, ticker_list, field_list, override_date):

		self.ticker_list = ticker_list
		self.field_list = field_list
		self.override_date = override_date
		self.data = pd.DataFrame()
		self.start_session()
		self.open_service()
	
	def open_service (self):
		
		if not self.session.openService("//blp/refdata"):
			print ("Failed to open //blp/refdata")
			return

		self.refDataService = self.session.getService("//blp/refdata")
	
	def request_data(self):
		
		cols = ["Ticker", "Weight", "Date"]
		data_list = []
		
		for t in self.ticker_list:
			try:

				for f in self.field_list:
				
					print ("Snaping data %s %s data..." % ( t, f ))
				
					request = self.refDataService.createRequest("ReferenceDataRequest")
					request.append("securities", t)
					request.append("fields", f)

					overrides = request.getElement("overrides")
					override1 = overrides.appendElement()
					override1.setElement("fieldId", "END_DATE_OVERRIDE")
					override1.setElement("value", self.override_date)

					self.session.sendRequest(request)

					while(True):
						ev = self.session.nextEvent()

						for msg in ev:
							if msg.hasElement(RESPONSE_ERROR):
								printErrorInfo("REQUEST FAILED: ", msg.getElement(RESPONSE_ERROR))
								continue
							if msg.messageType() in ["SessionConnectionUp", "SessionStarted", "ServiceOpened"] : 
								# log
								pass
							else:
								securityDataArray = msg.getElement(SECURITY_DATA)
								for securityData in securityDataArray.values():
									filedData = securityData.getElement(FIELD_DATA)
									for field in filedData.elements():
										for values in field.values():
											row = []
											row.append(self.override_date)
											for value in values.elements():
										 		row.append(value.getValueAsString())
											data_list.append(row)
								self.data = pd.DataFrame(data = data_list, columns = ['Date', 'Ticker', 'Weight'])

						if ev.eventType() == blpapi.Event.RESPONSE:
							break

			except:
				e = sys.exc_info()[0]
				print (e)
				continue
		
		return

if __name__ == '__main__':

	b = BbgSessionIndexWeight(['HSI Index'], ['INDX_MWEIGHT_HIST'], '20140930')
	b.request_data()
	print (b.data)