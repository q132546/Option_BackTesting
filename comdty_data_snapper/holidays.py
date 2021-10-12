from datetime import date, datetime, timedelta


AU = {
	# 2016
	date(2016,1,1):		"New Year's Day",
	date(2016,1,26):	"Australia Day Holiday",
	date(2016,3,25):	"Good Friday",
	date(2016,3,28):	"Easter Monday",
	date(2016,4,25):	"Anzac Day",
	date(2016,6,13):	"Queen's Birthday Holiday",
	date(2016,12,26):	"Christmas Holiday (obs)",
	date(2016,12,27):	"Boxing Day",

	# 2017
	date(2017,1,2):		"New Year's Day (obs)",
	date(2017,1,26):	"Australia Day Holiday",
	date(2017,4,14):	"Good Friday",
	date(2017,4,17):	"Easter Monday",
	date(2017,4,25):	"Anzac Day",
	date(2017,6,12):	"Queen's Birthday Holiday",
	date(2017,12,24):	"Christmas Eve",
	date(2017,12,25):	"Christmas Day",
	date(2017,12,26):	"Boxing Day",
	date(2017,12,31):	"New Year's Eve",

	}

HK = {
	# 2016
	date(2016,1,1):		"New Year's Day",
	date(2016,2,8):		"Lunar New Year",
	date(2016,2,9):		"Lunar New Year",
	date(2016,2,10):	"Lunar New Year",
	date(2016,3,25):	"Good Friday",
	date(2016,3,26):	"Holy Saturday",
	date(2016,3,28):	"Easter Monday",
	date(2016,4,4):		"Ching Ming Festival",
	date(2016,5,2):		"Labor Day (obs)",
	date(2016,5,14):	"Buddha's Birthday",
	date(2016,6,9):		"Tuen Ng Festival",
	date(2016,7,1):		"Sar Establishment Day",
	date(2016,8,2):		"No Trading",
	date(2016,8,2):		"No Settlements",
	date(2016,9,16):	"Day After Mid-autumn Fest",
	date(2016,10,1):	"National Day (obs)",
	date(2016,10,10):	"Chung Yeung Festival",
	date(2016,10,21):	"Closed - Typhoon",
	date(2016,12,24):	"Christmas Eve Obs-halfday",
	date(2016,12,26):	"Christmas Holiday (obs)",
	date(2016,12,27):	"Christmas Next Day",
	date(2016,12,31):	"New Years Eve-early Close",

	#2017
	date(2017,1,2):		"New Year's Day (obs)",
	date(2017,1,28):	"Lunar New Year",
	date(2017,1,29):	"Lunar New Year",
	date(2017,1,30):	"Lunar New Year",
	date(2017,1,31):	"Lunar New Year",
	date(2017,4,4):		"Ching Ming Festival",
	date(2017,4,14):	"Good Friday",
	date(2017,4,15):	"Holy Saturday",
	date(2017,4,17):	"Easter Monday",
	date(2017,5,1):		"Labour Day",
	date(2017,5,3):		"Buddha's Birthday",
	date(2017,5,30):	"Tuen Ng Festival",
	date(2017,7,1):		"Sar Establishment Day",
	date(2017,10,2):	"National Day (obs)",
	date(2017,10,5):	"Day After Mid-autumn Fest",
	date(2017,10,28):	"Chung Yeung Festival",
	date(2017,12,24):	"Christmas Eve Obs-halfday",
	date(2017,12,25):	"Christmas Day",
	date(2017,12,26):	"Christmas Holiday (obs)",
	date(2017,12,31):	"New Years Eve-early Close",
}

LN = {
	#2016
	date(2016,1,1):		"New Year's Day",
	date(2016,3,25):	"Good Friday",
	date(2016,3,28):	"Easter Monday",
	date(2016,5,2):		"Early May Bank Holiday",
	date(2016,5,30):	"Late May Bank Holiday",
	date(2016,8,29):	"Summer Bank Holiday",
	date(2016,12,26):	"Christmas Day (obs)",
	date(2016,12,27):	"Boxing Day (obs)",

	# 2017
	date(2017,1,2):		"New Year's Day Obs",
	date(2017,4,14):	"Good Friday",
	date(2017,4,17):	"Easter Monday",
	date(2017,5,1):		"Early May Bank Holiday",
	date(2017,5,29):	"Late May Bank Holiday",
	date(2017,8,28):	"Summer Bank Holiday",
	date(2017,12,25):	"Christmas Day",
	date(2017,12,26):	"Boxing Day",

}

US = {
	#2016
	date(2016,1,1):		"New Year's Day",
	date(2016,1,18):	"Martin L. King Day",
	date(2016,2,15):	"Presidents' Day",
	date(2016,3,25):	"Good Friday",
	date(2016,5,30):	"Memorial Day",
	date(2016,7,4):		"Independence Day",
	date(2016,9,5):		"Labor Day",
	date(2016,11,24):	"Thanksgiving",
	date(2016,12,26):	"Christmas Day (obs)",

	# 2017
	date(2017,1,2):		"New Year's Day (obs)",
	date(2017,1,16):	"Martin L. King Day",
	date(2017,2,20):	"Presidents' Day",
	date(2017,4,14):	"Good Friday",
	date(2017,5,29):	"Memorial Day",
	date(2017,7,4):		"Independence Day",
	date(2017,9,4):		"Labor Day",
	date(2017,11,11):	"Veterans' Day",
	date(2017,11,23):	"Thanksgiving",
	date(2017,12,24):	"Day Before Christmas",
	date(2017,12,25):	"Christmas Day",
	
}


def get_holiday ( mkt = "" ):
	'''
	mkt can be AU, HK, LN, US

	'''
	if mkt == "HK":
		return HK

	else:
		return []

def is_holiday (dt, mkt):

	'''
	mkt can be HK, ...

	return False if not a holiday
	return ( True, desc ) if a holiday

	'''	

	cal = get_holiday(mkt)

	if len(cal) > 0:
		if (dt in cal.keys()):
			return True, cal[dt]	# return description
		else:
			return False
	else: 
		return None

def prev_weekday_as_date(adate, market = ""):    # return previous weekday
	
	adate -= timedelta(days=1)

	if market == "":
		while adate.weekday() > 4: # Mon-Fri are 0-4
			adate -= timedelta(days=1)
	else:
		while adate.weekday() > 4 or is_holiday(adate, market): # Mon-Fri are 0-4
			adate -= timedelta(days=1)
	return adate


def prev_weekday_as_string(adate, market = "", format="%Y%m%d"):

	return prev_weekday_as_date(adate, market).strftime(format)


def next_weekday_as_date(adate, market = ""):    # return next weekday

	adate += timedelta(days=1)

	if market == "":
		while adate.weekday() > 4: # Mon-Fri are 0-4
			adate += timedelta(days=1)
	else:
		while adate.weekday() > 4 or is_holiday(adate, market): # Mon-Fri are 0-4
			adate += timedelta(days=1)
	return adate


def next_weekday_as_string(adate, market = "", format="%Y%m%d"):    # return next weekday

	return next_weekday_as_date(adate, market).strftime(format)