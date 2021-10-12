import blpapi

#bbg const
BAR_DATA = blpapi.Name("barData")
BAR_TICK_DATA = blpapi.Name("barTickData")
OPEN = blpapi.Name("open")
HIGH = blpapi.Name("high")
LOW = blpapi.Name("low")
CLOSE = blpapi.Name("close")
VOLUME = blpapi.Name("volume")
NUM_EVENTS = blpapi.Name("numEvents")
TIME = blpapi.Name("time")
RESPONSE_ERROR = blpapi.Name("responseError")
SESSION_TERMINATED = blpapi.Name("SessionTerminated")
CATEGORY = blpapi.Name("category")
MESSAGE = blpapi.Name("message")
SECURITY_DATA = blpapi.Name("securityData")
SECURITY_NAME = blpapi.Name("security")
FIELD_DATA	= blpapi.Name("fieldData")

COL_MAP = {
	
	'time': TIME, 
	'open': OPEN, 
	'high': HIGH, 
	'low': LOW, 
	'close': CLOSE, 
	'num_events': NUM_EVENTS, 
	'volume': VOLUME

}