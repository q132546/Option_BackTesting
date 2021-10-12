import os 
import h5py
import pandas as pd
import numpy as np
from datetime import datetime, date

dates = [date(2018, 2, 14), 
		 date(2018, 2, 15), 
		 date(2018, 2, 16), 
		 date(2018, 2, 19), 
		 date(2018, 2, 20), 
		 date(2018, 2, 21), 
		 ]

daily_dir = "s:\\bjin\\AHMinData\\daily\\"
yearly_dir = "s:\\bjin\\AHMinData\\yearly\\"

for d in dates:
	d_file = os.path.join(daily_dir, d.strftime("%Y%m%d") + ".hdf5")
	y_file = os.path.join(yearly_dir, str(d.year) + ".hdf5")
	
	d_hdf = h5py.File(d_file, 'a')
	y_hdf = h5py.File(y_file, 'a')

	for ticker in d_hdf.keys():
		if ticker in y_hdf.keys():

			ds_name = '/%s/%s' % (ticker, 'TRADE')
			dat_arr = d_hdf[ds_name][()]

			if len(dat_arr) > 0 :
				dat_arr_yr = y_hdf[ds_name][()]
				arr = np.concatenate((dat_arr_yr, dat_arr))
				del y_hdf[ds_name]
				data_set = y_hdf.create_dataset(ds_name, data = arr)

	y_hdf.flush()
	y_hdf.close()