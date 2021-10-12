from bloomberg import *
from datetime import date, datetime, timedelta
from constant import *

import os
import sys
import time

start = date.today() - timedelta(days = 150)
end = date.today() + timedelta(days = 1)
year = 2021 # date.today().year

root = "S:/bjin/FutMinData/Commodities/%s/"
contracts_file = "S:/mli/contracts.csv"
columns = ['time', 'open', 'high', 'low', 'close', 'volume']

contracts = pd.read_csv(contracts_file, index_col = 0, header = None)

for i in contracts.index:


	ticker_list = contracts.loc[i].tolist()
	
	root_prod = root % (i)
	
	# s = BbgSessionBar(ticker_list, ["TRADE"], 1, start, end, 'future', None, root_prod, columns)
	# data = s.request_data()
	#
	retry =10
	
	while retry>0:
		try:
			s = BbgSessionBar(ticker_list, ["TRADE"], 1, start, end, 'future', None, root_prod, columns)	
			data = s.request_data()
			
			break
		except:
			time.sleep(5)
			retry-=1
			print('Retrying : ')
	
	if retry==0:
		print("Error snapping data, resume next date")
		sys.exit(0)


	data['product'] = i

	if i in ['AG', 'UC']:
		data['contract'] = i + ' ' + data.contract.str[3] + data.year
	elif i in ['HG', 'CU', 'SI', 'HC', 'XU', 'HI']:
		data['contract'] = data.contract.str[:2] + ' ' + data.contract.str[2] + data.year
	elif i in ['FVS', 'UX', 'IFB', 'FFB', 'FFD', 'ES', 'VG', 'GX', 'CF', 'EO', 'ST', 'NK', 'TP', 'JPW', 'XP', 'PT', 'FT', 'TW', 'Z', 'AI', 'IH', 'NQ', 'IB', 'QZ']:
		data['contract'] = data.contract + ' Index'
	elif i in ['XID', 'JY', 'EC', 'DX']:
		data['contract'] = data.contract + ' Curncy'
	else:
		data['contract'] = data.contract + ' Comdty'
	del data['year']
	data.columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'contract', 'product']
	data.reset_index(inplace = True) 
    
	if i == 'HG':
		data['Open'] = data.Open / 100
		data['High'] = data.High / 100
		data['Low'] = data.Low / 100
		data['Close'] = data.Close / 100

	root_raw = root % (i) + str(year) + '.csv'
    
	if os.path.exists(root_raw):
		raw = pd.read_csv(root_raw).dropna()
		raw = pd.concat([raw, data], sort = False)
		raw.drop_duplicates(subset = ['time', 'contract'], inplace = True)
		raw.set_index('time', inplace = True)
		raw.index = pd.to_datetime(raw.index)
		raw.sort_index(inplace = True)

		# Check Data
		
		if raw.High.mean() > raw.Close.mean() > raw.Low.mean():
			raw.to_csv(root_raw)
			print (i + ' Done')
			print ('-' * 25)
		else:
			print (i + ' Data Concating Error!!!')
			sys.exit()

	else:
		data.set_index('time', inplace = True)
		data.to_csv(root_raw)

print("done")


