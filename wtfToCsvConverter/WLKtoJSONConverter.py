#Source: https://github.com/beamerblvd/weatherlink-python
"""
This module contains data definitions and data serialization/deserialization logic for data types (models) found in
WeatherLink.com data downloads, WeatherLink serial communications, and WeatherLink software database files.

The data formats in this file were obtained from Davis WeatherLink documentation in the following locations:
	- http://www.davisnet.com/support/weather/download/VantageSerialProtocolDocs_v261.pdf
	- C:/WeatherLink/Readme 6.0.rtf
"""

from __future__ import absolute_import

import collections
import datetime
import decimal
import enum
import struct

import six

import json


DASH_LARGE = 32767
DASH_LARGE_NEGATIVE = -32768
DASH_ZERO = 0
DASH_SMALL = 255

STRAIGHT_NUMBER = int

STRAIGHT_DECIMAL = decimal.Decimal

_TENTHS = decimal.Decimal('0.1')
TENTHS = lambda x: x * _TENTHS

_HUNDREDTHS = decimal.Decimal('0.01')
HUNDREDTHS = lambda x: x * _HUNDREDTHS

_THOUSANDTHS = decimal.Decimal('0.001')
THOUSANDTHS = lambda x: x * _THOUSANDTHS

_INCHES_PER_CENTIMETER = decimal.Decimal('0.393701')

def convert_datetime_to_timestamp(d):
	if isinstance(d, datetime.datetime):
		return ((d.day + (d.month * 32) + ((d.year - 2000) * 512)) << 16) + (d.minute + (d.hour * 100))
	return d


def convert_timestamp_to_datetime(timestamp):
	date_part = timestamp >> 16
	time_part = timestamp - (date_part << 16)

	year = int((date_part / 512) + 2000)
	month = int((date_part - ((year - 2000) * 512)) / 32)
	day = int(date_part - ((year - 2000) * 512) - (month * 32))
	hour = int(time_part / 100)
	minute = int(time_part - (hour * 100))

	return datetime.datetime(year, month, day, hour, minute)


@enum.unique
class BarometricTrend(enum.Enum):
	falling_rapidly = -60
	falling_slowly = -20
	steady = 0
	rising_slowly = 20
	rising_rapidly = 60


@enum.unique
class WindDirection(enum.Enum):
	NS = -1 #Eingefügt, da immer ein Fehler geworfen wurde
	N = 0
	NNE = 1
	NE = 2
	ENE = 3
	E = 4
	ESE = 5
	SE = 6
	SSE = 7
	S = 8
	SSW = 9
	SW = 10
	WSW = 11
	W = 12
	WNW = 13
	NW = 14
	NNW = 15

	def __init__(self, value):
		self.degrees = value * 22.5
		if self.degrees == 0.0:
			self.degrees = 360.0

	@staticmethod
	def from_degrees(degrees):
		if degrees < 1 or degrees > 360:
			return None
		return _WindDirection__FROM_DEGREE_MAP[degrees]
_WindDirection__FROM_DEGREE_MAP = {
	350: WindDirection.N, 351: WindDirection.N, 352: WindDirection.N, 353: WindDirection.N, 354: WindDirection.N, 355: WindDirection.N, 356: WindDirection.N, 357: WindDirection.N, 358: WindDirection.N, 359: WindDirection.N, 360: WindDirection.N, 1: WindDirection.N, 2: WindDirection.N, 3: WindDirection.N, 4: WindDirection.N, 5: WindDirection.N, 6: WindDirection.N, 7: WindDirection.N, 8: WindDirection.N, 9: WindDirection.N, 10: WindDirection.N, 11: WindDirection.N,  # noqa
	12: WindDirection.NNE, 13: WindDirection.NNE, 14: WindDirection.NNE, 15: WindDirection.NNE, 16: WindDirection.NNE, 17: WindDirection.NNE, 18: WindDirection.NNE, 19: WindDirection.NNE, 20: WindDirection.NNE, 21: WindDirection.NNE, 22: WindDirection.NNE, 23: WindDirection.NNE, 24: WindDirection.NNE, 25: WindDirection.NNE, 26: WindDirection.NNE, 27: WindDirection.NNE, 28: WindDirection.NNE, 29: WindDirection.NNE, 30: WindDirection.NNE, 31: WindDirection.NNE, 32: WindDirection.NNE, 33: WindDirection.NNE, 34: WindDirection.NNE,  # noqa
	35: WindDirection.NE, 36: WindDirection.NE, 37: WindDirection.NE, 38: WindDirection.NE, 39: WindDirection.NE, 40: WindDirection.NE, 41: WindDirection.NE, 42: WindDirection.NE, 43: WindDirection.NE, 44: WindDirection.NE, 45: WindDirection.NE, 46: WindDirection.NE, 47: WindDirection.NE, 48: WindDirection.NE, 49: WindDirection.NE, 50: WindDirection.NE, 51: WindDirection.NE, 52: WindDirection.NE, 53: WindDirection.NE, 54: WindDirection.NE, 55: WindDirection.NE, 56: WindDirection.NE,  # noqa
	57: WindDirection.ENE, 58: WindDirection.ENE, 59: WindDirection.ENE, 60: WindDirection.ENE, 61: WindDirection.ENE, 62: WindDirection.ENE, 63: WindDirection.ENE, 64: WindDirection.ENE, 65: WindDirection.ENE, 66: WindDirection.ENE, 67: WindDirection.ENE, 68: WindDirection.ENE, 69: WindDirection.ENE, 70: WindDirection.ENE, 71: WindDirection.ENE, 72: WindDirection.ENE, 73: WindDirection.ENE, 74: WindDirection.ENE, 75: WindDirection.ENE, 76: WindDirection.ENE, 77: WindDirection.ENE, 78: WindDirection.ENE, 79: WindDirection.ENE,  # noqa
	80: WindDirection.E, 81: WindDirection.E, 82: WindDirection.E, 83: WindDirection.E, 84: WindDirection.E, 85: WindDirection.E, 86: WindDirection.E, 87: WindDirection.E, 88: WindDirection.E, 89: WindDirection.E, 90: WindDirection.E, 91: WindDirection.E, 92: WindDirection.E, 93: WindDirection.E, 94: WindDirection.E, 95: WindDirection.E, 96: WindDirection.E, 97: WindDirection.E, 98: WindDirection.E, 99: WindDirection.E, 100: WindDirection.E, 101: WindDirection.E,  # noqa
	102: WindDirection.ESE, 103: WindDirection.ESE, 104: WindDirection.ESE, 105: WindDirection.ESE, 106: WindDirection.ESE, 107: WindDirection.ESE, 108: WindDirection.ESE, 109: WindDirection.ESE, 110: WindDirection.ESE, 111: WindDirection.ESE, 112: WindDirection.ESE, 113: WindDirection.ESE, 114: WindDirection.ESE, 115: WindDirection.ESE, 116: WindDirection.ESE, 117: WindDirection.ESE, 118: WindDirection.ESE, 119: WindDirection.ESE, 120: WindDirection.ESE, 121: WindDirection.ESE, 122: WindDirection.ESE, 123: WindDirection.ESE, 124: WindDirection.ESE,  # noqa
	125: WindDirection.SE, 126: WindDirection.SE, 127: WindDirection.SE, 128: WindDirection.SE, 129: WindDirection.SE, 130: WindDirection.SE, 131: WindDirection.SE, 132: WindDirection.SE, 133: WindDirection.SE, 134: WindDirection.SE, 135: WindDirection.SE, 136: WindDirection.SE, 137: WindDirection.SE, 138: WindDirection.SE, 139: WindDirection.SE, 140: WindDirection.SE, 141: WindDirection.SE, 142: WindDirection.SE, 143: WindDirection.SE, 144: WindDirection.SE, 145: WindDirection.SE, 146: WindDirection.SE,  # noqa
	147: WindDirection.SSE, 148: WindDirection.SSE, 149: WindDirection.SSE, 150: WindDirection.SSE, 151: WindDirection.SSE, 152: WindDirection.SSE, 153: WindDirection.SSE, 154: WindDirection.SSE, 155: WindDirection.SSE, 156: WindDirection.SSE, 157: WindDirection.SSE, 158: WindDirection.SSE, 159: WindDirection.SSE, 160: WindDirection.SSE, 161: WindDirection.SSE, 162: WindDirection.SSE, 163: WindDirection.SSE, 164: WindDirection.SSE, 165: WindDirection.SSE, 166: WindDirection.SSE, 167: WindDirection.SSE, 168: WindDirection.SSE, 169: WindDirection.SSE,  # noqa
	170: WindDirection.S, 171: WindDirection.S, 172: WindDirection.S, 173: WindDirection.S, 174: WindDirection.S, 175: WindDirection.S, 176: WindDirection.S, 177: WindDirection.S, 178: WindDirection.S, 179: WindDirection.S, 180: WindDirection.S, 181: WindDirection.S, 182: WindDirection.S, 183: WindDirection.S, 184: WindDirection.S, 185: WindDirection.S, 186: WindDirection.S, 187: WindDirection.S, 188: WindDirection.S, 189: WindDirection.S, 190: WindDirection.S, 191: WindDirection.S,  # noqa
	192: WindDirection.SSW, 193: WindDirection.SSW, 194: WindDirection.SSW, 195: WindDirection.SSW, 196: WindDirection.SSW, 197: WindDirection.SSW, 198: WindDirection.SSW, 199: WindDirection.SSW, 200: WindDirection.SSW, 201: WindDirection.SSW, 202: WindDirection.SSW, 203: WindDirection.SSW, 204: WindDirection.SSW, 205: WindDirection.SSW, 206: WindDirection.SSW, 207: WindDirection.SSW, 208: WindDirection.SSW, 209: WindDirection.SSW, 210: WindDirection.SSW, 211: WindDirection.SSW, 212: WindDirection.SSW, 213: WindDirection.SSW, 214: WindDirection.SSW,  # noqa
	215: WindDirection.SW, 216: WindDirection.SW, 217: WindDirection.SW, 218: WindDirection.SW, 219: WindDirection.SW, 220: WindDirection.SW, 221: WindDirection.SW, 222: WindDirection.SW, 223: WindDirection.SW, 224: WindDirection.SW, 225: WindDirection.SW, 226: WindDirection.SW, 227: WindDirection.SW, 228: WindDirection.SW, 229: WindDirection.SW, 230: WindDirection.SW, 231: WindDirection.SW, 232: WindDirection.SW, 233: WindDirection.SW, 234: WindDirection.SW, 235: WindDirection.SW, 236: WindDirection.SW,  # noqa
	237: WindDirection.WSW, 238: WindDirection.WSW, 239: WindDirection.WSW, 240: WindDirection.WSW, 241: WindDirection.WSW, 242: WindDirection.WSW, 243: WindDirection.WSW, 244: WindDirection.WSW, 245: WindDirection.WSW, 246: WindDirection.WSW, 247: WindDirection.WSW, 248: WindDirection.WSW, 249: WindDirection.WSW, 250: WindDirection.WSW, 251: WindDirection.WSW, 252: WindDirection.WSW, 253: WindDirection.WSW, 254: WindDirection.WSW, 255: WindDirection.WSW, 256: WindDirection.WSW, 257: WindDirection.WSW, 258: WindDirection.WSW, 259: WindDirection.WSW,  # noqa
	260: WindDirection.W, 261: WindDirection.W, 262: WindDirection.W, 263: WindDirection.W, 264: WindDirection.W, 265: WindDirection.W, 266: WindDirection.W, 267: WindDirection.W, 268: WindDirection.W, 269: WindDirection.W, 270: WindDirection.W, 271: WindDirection.W, 272: WindDirection.W, 273: WindDirection.W, 274: WindDirection.W, 275: WindDirection.W, 276: WindDirection.W, 277: WindDirection.W, 278: WindDirection.W, 279: WindDirection.W, 280: WindDirection.W, 281: WindDirection.W,  # noqa
	282: WindDirection.WNW, 283: WindDirection.WNW, 284: WindDirection.WNW, 285: WindDirection.WNW, 286: WindDirection.WNW, 287: WindDirection.WNW, 288: WindDirection.WNW, 289: WindDirection.WNW, 290: WindDirection.WNW, 291: WindDirection.WNW, 292: WindDirection.WNW, 293: WindDirection.WNW, 294: WindDirection.WNW, 295: WindDirection.WNW, 296: WindDirection.WNW, 297: WindDirection.WNW, 298: WindDirection.WNW, 299: WindDirection.WNW, 300: WindDirection.WNW, 301: WindDirection.WNW, 302: WindDirection.WNW, 303: WindDirection.WNW, 304: WindDirection.WNW,  # noqa
	305: WindDirection.NW, 306: WindDirection.NW, 307: WindDirection.NW, 308: WindDirection.NW, 309: WindDirection.NW, 310: WindDirection.NW, 311: WindDirection.NW, 312: WindDirection.NW, 313: WindDirection.NW, 314: WindDirection.NW, 315: WindDirection.NW, 316: WindDirection.NW, 317: WindDirection.NW, 318: WindDirection.NW, 319: WindDirection.NW, 320: WindDirection.NW, 321: WindDirection.NW, 322: WindDirection.NW, 323: WindDirection.NW, 324: WindDirection.NW, 325: WindDirection.NW, 326: WindDirection.NW,  # noqa
	327: WindDirection.NNW, 328: WindDirection.NNW, 329: WindDirection.NNW, 330: WindDirection.NNW, 331: WindDirection.NNW, 332: WindDirection.NNW, 333: WindDirection.NNW, 334: WindDirection.NNW, 335: WindDirection.NNW, 336: WindDirection.NNW, 337: WindDirection.NNW, 338: WindDirection.NNW, 339: WindDirection.NNW, 340: WindDirection.NNW, 341: WindDirection.NNW, 342: WindDirection.NNW, 343: WindDirection.NNW, 344: WindDirection.NNW, 345: WindDirection.NNW, 346: WindDirection.NNW, 347: WindDirection.NNW, 348: WindDirection.NNW, 349: WindDirection.NNW,  # noqa
}


class RainCollectorType(enum.Enum):
	pass


@enum.unique
class RainCollectorTypeSerial(RainCollectorType):
	inches_0_01 = 0x00
	millimeters_0_2 = 0x10
	millimeters_0_1 = 0x20
RainCollectorTypeSerial.inches_0_01.clicks_to_inches = lambda c: c * _HUNDREDTHS
RainCollectorTypeSerial.inches_0_01.clicks_to_centimeters = lambda c: c / _INCHES_PER_CENTIMETER * _HUNDREDTHS
RainCollectorTypeSerial.millimeters_0_2.clicks_to_inches = lambda c: c * _HUNDREDTHS * _INCHES_PER_CENTIMETER * 2
RainCollectorTypeSerial.millimeters_0_2.clicks_to_centimeters = lambda c: c * _HUNDREDTHS * 2
RainCollectorTypeSerial.millimeters_0_1.clicks_to_inches = lambda c: c * _HUNDREDTHS * _INCHES_PER_CENTIMETER
RainCollectorTypeSerial.millimeters_0_1.clicks_to_centimeters = lambda c: c * _HUNDREDTHS


@enum.unique
class RainCollectorTypeDatabase(RainCollectorType):
	inches_0_1 = 0x0000
	inches_0_01 = 0x1000
	millimeters_0_2 = 0x2000
	millimeters_1_0 = 0x3000
	millimeters_0_1 = 0x6000
RainCollectorTypeDatabase.inches_0_1.clicks_to_inches = lambda c: _TENTHS * c
RainCollectorTypeDatabase.inches_0_1.clicks_to_centimeters = lambda c: c / _INCHES_PER_CENTIMETER * _TENTHS
RainCollectorTypeDatabase.inches_0_01.clicks_to_inches = lambda c: _HUNDREDTHS * c
RainCollectorTypeDatabase.inches_0_01.clicks_to_centimeters = lambda c: c / _INCHES_PER_CENTIMETER * _HUNDREDTHS
RainCollectorTypeDatabase.millimeters_0_2.clicks_to_inches = lambda c: _HUNDREDTHS * _INCHES_PER_CENTIMETER * c * 2
RainCollectorTypeDatabase.millimeters_0_2.clicks_to_centimeters = lambda c: c * _HUNDREDTHS * 2
RainCollectorTypeDatabase.millimeters_1_0.clicks_to_inches = lambda c: _TENTHS * _INCHES_PER_CENTIMETER * c
RainCollectorTypeDatabase.millimeters_1_0.clicks_to_centimeters = lambda c: c * _TENTHS
RainCollectorTypeDatabase.millimeters_0_1.clicks_to_inches = lambda c: _HUNDREDTHS * _INCHES_PER_CENTIMETER * c
RainCollectorTypeDatabase.millimeters_0_1.clicks_to_centimeters = lambda c: c * _HUNDREDTHS


class RecordDict(dict):
	def __init__(self, *args, **kwargs):
		super(RecordDict, self).__init__(*args, **kwargs)

	def __getattr__(self, name):
		return self.__getitem__(name)

	def __setattr__(self, name, value):
		self[name] = value


class Header(RecordDict):
	VERSION_CODE_AND_COUNT_FORMAT = '=16sl'
	VERSION_CODE_AND_COUNT_LENGTH = 20

	def __init__(self, version_code, record_count, day_indexes):
		super(Header, self).__init__()
		self.version_code = version_code
		self.record_count = record_count
		self.day_indexes = day_indexes

	@classmethod
	def load_from_wlk(cls, file_handle):
		version_and_count = struct.unpack_from(
			cls.VERSION_CODE_AND_COUNT_FORMAT,
			file_handle.read(cls.VERSION_CODE_AND_COUNT_LENGTH),
		)
		day_indexes = []

		for i in range(0, 32):
			day_indexes.append(DayIndex.load_from_wlk(file_handle))

		return cls(version_and_count[0], version_and_count[1], day_indexes)


class DayIndex(RecordDict):
	DAY_INDEX_FORMAT = '=hl'
	DAY_INDEX_LENGTH = 6

	def __init__(self, record_count, start_index):
		super(DayIndex, self).__init__()
		self.record_count = record_count
		self.start_index = start_index

	@classmethod
	def load_from_wlk(cls, file_handle):
		return cls(*struct.unpack_from(
			cls.DAY_INDEX_FORMAT,
			file_handle.read(cls.DAY_INDEX_LENGTH),
		))


class DailySummary(RecordDict):
	DAILY_SUMMARY_FORMAT = (
		'=bx'  # '2' plus a reserved byte [ignored]
		'h'  # number of minutes accounted for in this day's records
		'2h'  # hi and low outside temps in tenths of degres
		'2h'  # hi and low inside temps in tenths of degrees
		'h'  # average outside temp in tenths of degrees
		'h'  # average inside temp in tenths of degrees
		'2h'  # hi and low wind chill temps in tenths of degrees
		'2h'  # hi and low dew point temps in tenths of degrees
		'h'  # average wind chill temp in tenths of degrees
		'h'  # average dew point temp in tenths of degrees
		'2h'  # hi and low outside humitidy in tenths of percents
		'2h'  # hi and low inside humitidy in tenths of percents
		'h'  # average outside humitidy in tenths of percents
		'2h'  # hi and low barometric pressure in thousandths of inches of mercury
		'h'  # average barometric pressure in thousandths of inches of mercury
		'h'  # high wind speed in tenths of miles per hour
		'h'  # average wind speed in tenths of miles per hour
		'h'  # daily wind run total in tenths of miles
		'h'  # highest 10-minute average wind speed in tenths of miles per hour
		'b'  # direction code (0-15, 255) for high wind speed
		'b'  # direction code for highest 10-minute average wind speed
		'h'  # daily rain total in thousandths of inches
		'h'  # hi daily rain rate in hundredths of inches per hour
		'2x'  # daily UV dose (ignored)
		'x'  # high UV dose (ignored)
		'27x'  # time values (ignored)
		'bx'  # '3' plus a reserved byte [ignored]
		'2x'  # today's weather (unsupported and ignored)
		'h'  # total number of wind packets
		'8x'  # solar, sunlight, and evapotranspiration (ignored)
		'2h'  # hi and low heat index in tenths of degrees
		'h'  # average heat index in tenths of degrees
		'4x'  # hi and low temperature-humidity-sun-wind (THSW) index in tenths of degrees (ignored)
		'2h'  # hi and low temperature-humidity-wind (THW) index in tenths of degrees
		'h'  # integrated heating degree days in tenths of degrees
		'2h'  # hi and low wet bulb temp in tenths of degrees
		'h'  # average wet bulb temp in tenths of degrees
		'24x'  # unused space for direction bins (ignored)
		'15x'  # unused space for time values (ignored)
		'h'  # integrated cooling degree days in tenths of degrees
		'11x'  # reserved bytes (ignored)
	)
	DAILY_SUMMARY_LENGTH = 88 * 2
	DAILY_SUMMARY_VERIFICATION_MAP = {
		0: 2,
		30: 3,
	}
	DAILY_SUMMARY_ATTRIBUTE_MAP = (
		('ds1_version', STRAIGHT_NUMBER, None, ),
		('minutes', STRAIGHT_NUMBER, None, ),
		('temperature_outside_high', TENTHS, DASH_LARGE_NEGATIVE, ),
		('temperature_outside_low', TENTHS, DASH_LARGE, ),
		('temperature_inside_high', TENTHS, DASH_LARGE_NEGATIVE, ),
		('temperature_inside_low', TENTHS, DASH_LARGE, ),
		('temperature_outside_average', TENTHS, DASH_LARGE, ),
		('temperature_inside_average', TENTHS, DASH_LARGE, ),
		('wind_chill_high', TENTHS, DASH_LARGE_NEGATIVE, ),
		('wind_chill_low', TENTHS, DASH_LARGE, ),
		('dew_point_high', TENTHS, DASH_LARGE_NEGATIVE, ),
		('dew_point_low', TENTHS, DASH_LARGE, ),
		('wind_chill_average', TENTHS, DASH_LARGE, ),
		('dew_point_average', TENTHS, DASH_LARGE, ),
		('humidity_outside_high', TENTHS, DASH_SMALL, ),
		('humidity_outside_low', TENTHS, DASH_SMALL, ),
		('humidity_inside_high', TENTHS, DASH_SMALL, ),
		('humidity_inside_low', TENTHS, DASH_SMALL, ),
		('humidity_outside_average', TENTHS, DASH_SMALL, ),
		('barometric_pressure_high', THOUSANDTHS, DASH_ZERO, ),
		('barometric_pressure_low', THOUSANDTHS, DASH_ZERO, ),
		('barometric_pressure_average', THOUSANDTHS, DASH_ZERO, ),
		('wind_speed_high', TENTHS, DASH_ZERO, ),
		('wind_speed_average', TENTHS, DASH_ZERO, ),
		('wind_daily_run', TENTHS, DASH_ZERO, ),
		('wind_speed_high_10_minute_average', TENTHS, DASH_LARGE_NEGATIVE, ),
		('wind_speed_high_direction', WindDirection, DASH_SMALL, ),
		('wind_speed_high_10_minute_average_direction', WindDirection, DASH_SMALL, ),
		('rain_total', THOUSANDTHS, None, ),
		('rain_rate_high', HUNDREDTHS, None, ),
		('ds2_version', STRAIGHT_NUMBER, None, ),
		('total_wind_packets', STRAIGHT_NUMBER, DASH_LARGE_NEGATIVE, ),
		('heat_index_high', TENTHS, DASH_LARGE_NEGATIVE, ),
		('heat_index_low', TENTHS, DASH_LARGE, ),
		('heat_index_average', TENTHS, DASH_LARGE, ),
		('thw_index_high', TENTHS, DASH_LARGE_NEGATIVE, ),
		('thw_index_low', TENTHS, DASH_LARGE, ),
		('integrated_heating_degree_days', TENTHS, DASH_ZERO, ),
		('temperature_wet_bulb_high', TENTHS, DASH_LARGE_NEGATIVE, ),
		('temperature_wet_bulb_low', TENTHS, DASH_LARGE, ),
		('temperature_wet_bulb_average', TENTHS, DASH_LARGE, ),
		('integrated_cooling_degree_days', TENTHS, DASH_ZERO, ),
	)

	def __init__(self, *args, **kwargs):
		super(DailySummary, self).__init__(*args, **kwargs)

	@classmethod
	def load_from_wlk(cls, file_handle, year, month, day):
		arguments = struct.unpack_from(
			cls.DAILY_SUMMARY_FORMAT,
			file_handle.read(cls.DAILY_SUMMARY_LENGTH),
		)

		for k, v in six.iteritems(cls.DAILY_SUMMARY_VERIFICATION_MAP):
			if arguments[k] != v:
				raise AssertionError('{} did not match expected {}'.format(arguments[k], v))

		kwargs = {}
		for i, v in enumerate(arguments):
			if i not in cls.DAILY_SUMMARY_VERIFICATION_MAP:
				k = cls.DAILY_SUMMARY_ATTRIBUTE_MAP[i][0]
				if v == cls.DAILY_SUMMARY_ATTRIBUTE_MAP[i][2]:
					kwargs[k] = None
				else:
					kwargs[k] = cls.DAILY_SUMMARY_ATTRIBUTE_MAP[i][1](v)

		return cls(date=datetime.date(year, month, day), **kwargs)


class ArchiveIntervalRecord(RecordDict):
	RECORD_FORMAT_WLK = (
		'=b'  # '1'
		'b'  # minutes in this record
		'2x'  # icon flags and oter flags (ignored)
		'h'  # minutes past midnight
		'h'  # current outside temp in tenths of degrees
		'h'  # hi outside temp this time period in tenths of degrees
		'h'  # low outside temp this time period in tenths of degrees
		'h'  # current inside temp in tenths of degrees
		'h'  # barometric pressure in thousandths of inches of mercury
		'h'  # outside humidity in tenths of percents
		'h'  # inside humidity in tenths of percents
		'H'  # raw rain clicks (clicks masked with type)
		'h'  # high rain rate this time period in raw clicks/hr
		'h'  # wind speed in tenths of miles per hour
		'h'  # hi wind speed this time period in tenths of miles per hour
		'b'  # prevailing wind direction (0-15, 255)
		'b'  # hi wind speed direction (0-15, 255)
		'h'  # number of wind samples this time period
		'h'  # average solar rad this time period in watts / meter squared
		'h'  # high solar radiation this time period in watts / meter squared
		'B'  # UV index
		'B'  # high UV index during this time period
		'50x'  # other unused items (ignored)
	)
	RECORD_FORMAT_DOWNLOAD = (
		'=hh'  # date and time stamps
		'h'  # current outside temp in tenths of degrees
		'h'  # hi outside temp this time period in tenths of degrees
		'h'  # low outside temp this time period in tenths of degrees
		'H'  # raw rain clicks (clicks masked with type)
		'H'  # high rain rate this time period in raw clicks/hr
		'H'  # barometric pressure in thousandths of inches of mercury
		'h'  # average solar rad this time period in watts / meter squared
		'H'  # number of wind samples this time period
		'h'  # current inside temp in tenths of degrees
		'B'  # inside humidity in tenths of percents
		'B'  # outside humidity in tenths of percents
		'B'  # wind speed in tenths of miles per hour
		'B'  # hi wind speed this time period in tenths of miles per hour
		'B'  # hi wind speed direction (0-15, 255)
		'B'  # prevailing wind direction (0-15, 255)
		'B'  # UV index
		'B'  # evapotranspiration in thousandths of inches (only during hour on top of hour)
		'h'  # high solar radiation this time period in watts / meter squared
		'B'  # high UV index during this time period
		'9x'  # unused for now (ignored)
		'B'  # Download record type (0xFF = Rev A, 0x00 = Rev B)
		'9x'  # unused for now (ignored)
	)
	RECORD_LENGTH_WLK = 88
	RECORD_LENGTH_DOWNLOAD = 52
	RECORD_VERIFICATION_MAP_WLK = {
		0: 1,
	}
	RECORD_VERIFICATION_MAP_DOWNLOAD = {
		21: 0,
	}
	RECORD_SPECIAL_HANDLING_WLK = {10, 11}
	RECORD_SPECIAL_HANDLING_DOWNLOAD = {0, 1, 5, 6}
	RECORD_ATTRIBUTE_MAP_WLK = (
		('record_version', STRAIGHT_NUMBER, None, ),
		('minutes_covered', STRAIGHT_NUMBER, None, ),
		('minutes_past_midnight', STRAIGHT_NUMBER, None, ),
		('temperature_outside', TENTHS, DASH_LARGE, ),
		('temperature_outside_high', TENTHS, DASH_LARGE_NEGATIVE, ),
		('temperature_outside_low', TENTHS, DASH_LARGE, ),
		('temperature_inside', TENTHS, DASH_LARGE, ),
		('barometric_pressure', THOUSANDTHS, DASH_ZERO, ),
		('humidity_outside', TENTHS, DASH_SMALL, ),
		('humidity_inside', TENTHS, DASH_SMALL, ),
		('__special', STRAIGHT_NUMBER, None, ),
		('__special', STRAIGHT_NUMBER, None, ),
		('wind_speed', TENTHS, DASH_SMALL, ),
		('wind_speed_high', TENTHS, DASH_ZERO, ),
		('wind_direction_prevailing', WindDirection, DASH_SMALL, ),
		('wind_direction_speed_high', WindDirection, DASH_SMALL, ),
		('number_of_wind_samples', STRAIGHT_NUMBER, DASH_ZERO, ),
		('solar_radiation', STRAIGHT_NUMBER, DASH_LARGE_NEGATIVE, ),
		('solar_radiation_high', STRAIGHT_NUMBER, DASH_LARGE_NEGATIVE, ),
		('uv_index', TENTHS, DASH_SMALL, ),
		('uv_index_high', TENTHS, DASH_SMALL, ),
	)
	RECORD_ATTRIBUTE_MAP_DOWNLOAD = (
		('__special', STRAIGHT_NUMBER, None, ),
		('__special', STRAIGHT_NUMBER, None, ),
		('temperature_outside', TENTHS, DASH_LARGE, ),
		('temperature_outside_high', TENTHS, DASH_LARGE_NEGATIVE, ),
		('temperature_outside_low', TENTHS, DASH_LARGE, ),
		('__special', STRAIGHT_NUMBER, None, ),
		('__special', STRAIGHT_NUMBER, None, ),
		('barometric_pressure', THOUSANDTHS, DASH_ZERO, ),
		('solar_radiation', STRAIGHT_NUMBER, DASH_LARGE, ),
		('number_of_wind_samples', STRAIGHT_NUMBER, DASH_ZERO, ),
		('temperature_inside', TENTHS, DASH_LARGE, ),
		('humidity_inside', STRAIGHT_NUMBER, DASH_SMALL, ),
		('humidity_outside', STRAIGHT_NUMBER, DASH_SMALL, ),
		('wind_speed', STRAIGHT_DECIMAL, DASH_SMALL, ),
		('wind_speed_high', STRAIGHT_DECIMAL, DASH_ZERO, ),
		('wind_direction_speed_high', WindDirection, DASH_SMALL, ),
		('wind_direction_prevailing', WindDirection, DASH_SMALL, ),
		('uv_index', TENTHS, DASH_SMALL, ),
		('evapotranspiration', THOUSANDTHS, DASH_ZERO, ),
		('solar_radiation_high', STRAIGHT_NUMBER, DASH_LARGE, ),
		('uv_index_high', TENTHS, DASH_SMALL, ),
		('record_version', STRAIGHT_NUMBER, None, ),
	)

	RECORD_WIND_DIRECTION_SPECIAL = (
		('wind_direction_prevailing', 'wind_direction_prevailing_degrees', ),
		('wind_direction_speed_high', 'wind_direction_speed_high_degrees', ),
	)

	@classmethod
	def load_from_wlk(cls, file_handle, year, month, day):
		arguments = struct.unpack_from(
			cls.RECORD_FORMAT_WLK,
			file_handle.read(cls.RECORD_LENGTH_WLK),
		)

		for k, v in six.iteritems(cls.RECORD_VERIFICATION_MAP_WLK):
			if arguments[k] != v:
				raise AssertionError('{} did not match expected {}'.format(arguments[k], v))

		kwargs = {}
		for i, v in enumerate(arguments):
			if i not in cls.RECORD_VERIFICATION_MAP_WLK and i not in cls.RECORD_SPECIAL_HANDLING_WLK:
				k = cls.RECORD_ATTRIBUTE_MAP_WLK[i][0]
				if v == cls.RECORD_ATTRIBUTE_MAP_WLK[i][2]:
					kwargs[k] = None
				else:
					kwargs[k] = cls.RECORD_ATTRIBUTE_MAP_WLK[i][1](v)

		record = cls(**kwargs)

		rain_code = arguments[10]
		rain_collector_type = rain_code & 0xF000
		rain_clicks = rain_code & 0x0FFF
		rain_rate_clicks = arguments[11]

		record.rain_collector_type = RainCollectorTypeDatabase(rain_collector_type)
		record.rain_amount_clicks = rain_clicks
		record.rain_rate_clicks = rain_rate_clicks
		record.rain_amount = record.rain_collector_type.clicks_to_inches(rain_clicks)
		record.rain_rate = record.rain_collector_type.clicks_to_inches(rain_rate_clicks)

		for k1, k2 in cls.RECORD_WIND_DIRECTION_SPECIAL:
			if record[k1]:
				record[k2] = record[k1].degrees
			else:
				record[k2] = None

		record.date = (
			datetime.datetime(year, month, day, 0, 0) + datetime.timedelta(minutes=record.minutes_past_midnight)
		)
		record.timestamp = convert_datetime_to_timestamp(record.date)

		return record

	@classmethod
	def load_from_download(cls, response_handle, minutes_covered):
		arguments = struct.unpack_from(
			cls.RECORD_FORMAT_DOWNLOAD,
			response_handle.read(cls.RECORD_LENGTH_DOWNLOAD),
		)
		if arguments[0] < 1:
			print('WARN: Record ignored due to datestamp < 1: date %s, time %s' % (arguments[0], arguments[1]))
			return None

		for k, v in six.iteritems(cls.RECORD_VERIFICATION_MAP_DOWNLOAD):
			if arguments[k] != v:
				raise AssertionError('{} did not match expected {}'.format(arguments[k], v))

		kwargs = {}
		for i, v in enumerate(arguments):
			if i not in cls.RECORD_VERIFICATION_MAP_DOWNLOAD and i not in cls.RECORD_SPECIAL_HANDLING_DOWNLOAD:
				k = cls.RECORD_ATTRIBUTE_MAP_DOWNLOAD[i][0]
				if v == cls.RECORD_ATTRIBUTE_MAP_DOWNLOAD[i][2]:
					kwargs[k] = None
				else:
					kwargs[k] = cls.RECORD_ATTRIBUTE_MAP_DOWNLOAD[i][1](v)

		record = cls(**kwargs)

		record.minutes_covered = minutes_covered

		rain_clicks = arguments[5]
		rain_rate_clicks = arguments[6]

		record.rain_collector_type = RainCollectorTypeSerial.inches_0_01
		record.rain_amount_clicks = rain_clicks
		record.rain_rate_clicks = rain_rate_clicks
		record.rain_amount = record.rain_collector_type.clicks_to_inches(rain_clicks)
		record.rain_rate = record.rain_collector_type.clicks_to_inches(rain_rate_clicks)

		for k1, k2 in cls.RECORD_WIND_DIRECTION_SPECIAL:
			if record[k1]:
				record[k2] = record[k1].degrees
			else:
				record[k2] = None

		record.timestamp = (arguments[0] << 16) + arguments[1]
		record.date = convert_timestamp_to_datetime(record.timestamp)

		return record


class LoopRecord(RecordDict):
	RECORD_LENGTH = 99

	LOOP1_RECORD_TYPE = 0
	LOOP2_RECORD_TYPE = 1

	LOOP2_RECORD_FORMAT = (
		'<3s'  # String 'LOO'
		'b'  # barometer trend
		'B'  # Should be 1 for "LOOP 2" (0 would indicate "LOOP 1")
		'H'  # Unused, should be 0x7FFF
		'H'  # Barometer in thousandths of inches of mercury
		'h'  # Inside temperature in tenths of degrees Fahrenheit
		'B'  # Inside humidity in whole percents
		'h'  # Outside temperature in tenths of degrees Fahrenheit
		'B'  # Wind speed in MPH
		'B'  # Unused, should be 0xFF
		'H'  # Wind direction in degrees, 0 = no wind, 1 = nearly N, 90 = E, 180 = S, 270 = W, 360 = N
		'H'  # 10-minute wind average speed in tenths of MPH
		'H'  # 2-minute wind average speed in tenths of MPH
		'H'  # 10-minute wind gust speed in tenths of MPH
		'H'  # 10-minute wind gust direction in degrees
		'H'  # Unused, should be 0x7FFF
		'H'  # Unused, should be 0x7FFF
		'h'  # Dew point in whole degrees Fahrenheit
		'B'  # Unused, should be 0xFF
		'B'  # Outside humidity in whole percents
		'B'  # Unused, should be 0xFF
		'h'  # Heat index in whole degrees Fahrenheit
		'h'  # Wind chill in whole degrees Fahrenheit
		'h'  # THSW index in whole degrees Fahrenheit
		'H'  # Rain rate in clicks/hour
		'B'  # UV Index
		'H'  # Solar radiation in watts per square meter
		'H'  # Number of rain clicks this storm
		'2x'  # Useless start date of this storm, which we don't care about
		'H'  # Number of rain clicks today
		'H'  # Number of rain clicks last 15 minutes
		'H'  # Number of rain clicks last 1 hour
		'H'  # Daily total evapotranspiration in thousandths of inches
		'H'  # Number of rain clicks last 24 hours
		'11x'  # Barometer calibration-related settings and readings
		'B'  # Unused, should be 0xFF
		'x'  # Unused field filled with undefined data
		'6x'  # Information about what's displayed on the console graph, which we don't care about
		'B'  # The minute within the hour, 0-59
		'3x'  # Information about what's displayed on the console graph, which we don't care about
		'H'  # Unused, should be 0x7FFF
		'H'  # Unused, should be 0x7FFF
		'H'  # Unused, should be 0x7FFF
		'H'  # Unused, should be 0x7FFF
		'H'  # Unused, should be 0x7FFF
		'H'  # Unused, should be 0x7FFF
		'c'  # Should be '\n'
		'c'  # Should be '\r'
		'H'  # Cyclic redundancy check (CRC)
	)

	LOOP2_RECORD_VERIFICATION_MAP_WLK = {
		0: b'LOO',
		2: 1,
		3: 0x7FFF,
		9: 0xFF,
		15: 0x7FFF,
		16: 0x7FFF,
		18: 0xFF,
		20: 0xFF,
		33: 0xFF,
		35: 0x7FFF,
		36: 0x7FFF,
		37: 0x7FFF,
		38: 0x7FFF,
		39: 0x7FFF,
		40: 0x7FFF,
		41: b'\n',
		42: b'\r',
	}

	LOOP2_RECORD_SPECIAL_HANDLING = frozenset(LOOP2_RECORD_VERIFICATION_MAP_WLK.keys())

	LOOP2_RECORD_ATTRIBUTE_MAP = (
		('_special', STRAIGHT_NUMBER, None, ),
		('barometric_trend', STRAIGHT_NUMBER, 80, ),
		('_special', STRAIGHT_NUMBER, None, ),
		('_special', STRAIGHT_NUMBER, None, ),
		('barometric_pressure', THOUSANDTHS, DASH_ZERO, ),
		('temperature_inside', TENTHS, DASH_LARGE, ),
		('humidity_inside', STRAIGHT_NUMBER, DASH_SMALL, ),
		('temperature_outside', TENTHS, DASH_LARGE, ),
		('wind_speed', STRAIGHT_DECIMAL, DASH_SMALL, ),
		('_special', STRAIGHT_NUMBER, None, ),
		('wind_direction_degrees', STRAIGHT_NUMBER, DASH_ZERO, ),
		('wind_speed_10_minute_average', TENTHS, DASH_ZERO, ),
		('wind_speed_2_minute_average', TENTHS, DASH_ZERO, ),
		('wind_speed_10_minute_gust', TENTHS, DASH_ZERO, ),
		('wind_speed_10_minute_gust_direction_degrees', STRAIGHT_NUMBER, DASH_ZERO, ),
		('_special', STRAIGHT_NUMBER, None, ),
		('_special', STRAIGHT_NUMBER, None, ),
		('dew_point', STRAIGHT_DECIMAL, DASH_SMALL, ),
		('_special', STRAIGHT_NUMBER, None, ),
		('humidity_outside', STRAIGHT_NUMBER, DASH_SMALL, ),
		('_special', STRAIGHT_NUMBER, None, ),
		('heat_index', STRAIGHT_NUMBER, DASH_SMALL, ),
		('wind_chill', STRAIGHT_NUMBER, DASH_SMALL, ),
		('thsw_index', STRAIGHT_NUMBER, DASH_SMALL, ),
		('rain_rate_clicks', STRAIGHT_NUMBER, None, ),
		('uv_index', TENTHS, DASH_SMALL, ),
		('solar_radiation', STRAIGHT_NUMBER, DASH_LARGE, ),
		('rain_clicks_this_storm', STRAIGHT_NUMBER, None, ),
		('rain_clicks_today', STRAIGHT_NUMBER, None, ),
		('rain_clicks_15_minutes', STRAIGHT_NUMBER, None, ),
		('rain_clicks_1_hour', STRAIGHT_NUMBER, None, ),
		('evapotranspiration', THOUSANDTHS, DASH_ZERO, ),
		('rain_clicks_24_hours', STRAIGHT_NUMBER, None, ),
		('_special', STRAIGHT_NUMBER, None),
		('minute_in_hour', STRAIGHT_NUMBER, 60, ),
	)

	LOOP_WIND_DIRECTION_SPECIAL = (
		('wind_direction_degrees', 'wind_direction', ),
		('wind_speed_10_minute_gust_direction_degrees', 'wind_speed_10_minute_gust_direction', ),
	)

	LOOP_RAIN_AMOUNT_SPECIAL = (
		('rain_rate_clicks', 'rain_rate', ),
		('rain_clicks_this_storm', 'rain_amount_this_storm', ),
		('rain_clicks_today', 'rain_amount_today', ),
		('rain_clicks_15_minutes', 'rain_amount_15_minutes', ),
		('rain_clicks_1_hour', 'rain_amount_1_hour', ),
		('rain_clicks_24_hours', 'rain_amount_24_hours', ),
	)

	@classmethod
	def load_loop_1_2_from_connection(cls, socket_file):
		arguments = cls._get_loop_1_arguments(socket_file, True)
		arguments.update(cls._get_loop_2_arguments(socket_file))
		return cls(**arguments)

	@classmethod
	def load_loop_1_from_connection(cls, socket_file):
		return cls(**cls._get_loop_1_arguments(socket_file))

	@classmethod
	def load_loop_2_from_connection(cls, socket_file):
		return cls(**cls._get_loop_2_arguments(socket_file))

	@classmethod
	def _get_loop_1_arguments(cls, socket_file, unique_only=False):
		raise NotImplementedError()

	@classmethod
	def _get_loop_2_arguments(cls, socket_file):
		data = socket_file.read(cls.RECORD_LENGTH)

		unpacked = struct.unpack_from(cls.LOOP2_RECORD_FORMAT, data)

		for k, v in six.iteritems(cls.LOOP2_RECORD_VERIFICATION_MAP_WLK):
			if unpacked[k] != v:
				raise AssertionError('{} did not match expected {}'.format(unpacked[k], v))

		arguments = {'crc_match': calculate_weatherlink_crc(data) == 0, 'record_type': 2}

		last = len(cls.LOOP2_RECORD_ATTRIBUTE_MAP)
		for i, v in enumerate(unpacked):
			if (i < last and i not in cls.LOOP2_RECORD_VERIFICATION_MAP_WLK and
						i not in cls.LOOP2_RECORD_SPECIAL_HANDLING):
				k = cls.LOOP2_RECORD_ATTRIBUTE_MAP[i][0]
				if v == cls.LOOP2_RECORD_ATTRIBUTE_MAP[i][2]:
					arguments[k] = None
				else:
					arguments[k] = cls.LOOP2_RECORD_ATTRIBUTE_MAP[i][1](v)

		cls._post_process_arguments(arguments)

		return arguments

	@classmethod
	def _post_process_arguments(cls, arguments):
		# The online download does not contain this information, unfortunately
		rain_collector_type = RainCollectorTypeSerial.inches_0_01

		try:
			arguments['barometric_trend'] = BarometricTrend(arguments['barometric_trend'])
		except ValueError:
			arguments['barometric_trend'] = None

		for k1, k2 in cls.LOOP_WIND_DIRECTION_SPECIAL:
			if arguments[k1]:
				arguments[k2] = WindDirection.from_degrees(arguments[k1])
			else:
				arguments[k2] = None

		for k1, k2 in cls.LOOP_RAIN_AMOUNT_SPECIAL:
			if arguments[k1]:
				arguments[k2] = rain_collector_type.clicks_to_inches(arguments[k1])
			else:
				arguments[k2] = None


WEATHERLINK_CRC_TABLE = (
	0x0, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
	0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
	0x1231, 0x210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
	0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
	0x2462, 0x3443, 0x420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
	0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
	0x3653, 0x2672, 0x1611, 0x630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
	0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
	0x48c4, 0x58e5, 0x6886, 0x78a7, 0x840, 0x1861, 0x2802, 0x3823,
	0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
	0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0xa50, 0x3a33, 0x2a12,
	0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
	0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0xc60, 0x1c41,
	0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
	0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0xe70,
	0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
	0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
	0x1080, 0xa1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
	0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
	0x2b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
	0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
	0x34e2, 0x24c3, 0x14a0, 0x481, 0x7466, 0x6447, 0x5424, 0x4405,
	0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
	0x26d3, 0x36f2, 0x691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
	0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
	0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x8e1, 0x3882, 0x28a3,
	0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
	0x4a75, 0x5a54, 0x6a37, 0x7a16, 0xaf1, 0x1ad0, 0x2ab3, 0x3a92,
	0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
	0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0xcc1,
	0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
	0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0xed1, 0x1ef0,
)


def calculate_weatherlink_crc(data_bytes):
	crc = 0
	cast_with_ord = isinstance(data_bytes, six.string_types)
	for i, byte in enumerate(data_bytes):
		if cast_with_ord:
			byte = ord(byte)
		crc = WEATHERLINK_CRC_TABLE[((crc >> 8) & 0xFF) ^ byte] ^ ((crc << 8) & 0xFF00)
	return crc

##### Utilities #####
ZERO = decimal.Decimal('0')
ONE = decimal.Decimal('1')
TWO = decimal.Decimal('2')
FOUR = decimal.Decimal('4')
FIVE = decimal.Decimal('5')
TEN = decimal.Decimal('10')
ONE_TENTH = decimal.Decimal('0.1')
ONE_HUNDREDTH = ONE_TENTH * ONE_TENTH
ONE_THOUSANDTH = ONE_TENTH * ONE_HUNDREDTH
FIVE_NINTHS = decimal.Decimal('5.0') / decimal.Decimal('9.0')
NINE_FIFTHS = decimal.Decimal('9.0') / decimal.Decimal('5.0')

CELSIUS_CONSTANT = decimal.Decimal('32')
KELVIN_CONSTANT = decimal.Decimal('459.67')
KILOPASCAL_MERCURY_CONSTANT = decimal.Decimal('0.295299830714')
MILLIBAR_MERCURY_CONSTANT = KILOPASCAL_MERCURY_CONSTANT * ONE_TENTH
METERS_PER_SECOND_CONSTANT = decimal.Decimal('0.44704')

# Wet bulb constants used by NOAA/NWS in its wet bulb temperature charts
WB_0_00066 = decimal.Decimal('0.00066')
WB_0_007 = decimal.Decimal('0.007')
WB_0_114 = decimal.Decimal('0.114')
WB_0_117 = decimal.Decimal('0.117')
WB_2_5 = decimal.Decimal('2.5')
WB_6_11 = decimal.Decimal('6.11')
WB_7_5 = decimal.Decimal('7.5')
WB_14_55 = decimal.Decimal('14.55')
WB_15_9 = decimal.Decimal('15.9')
WB_237_7 = decimal.Decimal('237.7')

# Dew point constants used by NOAA/NWS in the August-Roche-Magnus approximation with the Bogel modification
DP_A = decimal.Decimal('6.112')  # millibars
DP_B = decimal.Decimal('17.67')  # no units
DP_C = decimal.Decimal('243.5')  # degrees Celsius
DP_D = decimal.Decimal('234.5')  # degrees Celsius

# Heat index constants used by NOAA/NWS in its heat index tables
HI_SECOND_FORMULA_THRESHOLD = decimal.Decimal('80.0')
HI_0_094 = decimal.Decimal('0.094')
HI_0_5 = decimal.Decimal('0.5')
HI_1_2 = decimal.Decimal('1.2')
HI_61_0 = decimal.Decimal('61.0')
HI_68_0 = decimal.Decimal('68.0')
HI_C1 = decimal.Decimal('-42.379')
HI_C2 = decimal.Decimal('2.04901523')
HI_C3 = decimal.Decimal('10.14333127')
HI_C4 = decimal.Decimal('-0.22475541')
HI_C5 = decimal.Decimal('-0.00683783')
HI_C6 = decimal.Decimal('-0.05481717')
HI_C7 = decimal.Decimal('0.00122874')
HI_C8 = decimal.Decimal('0.00085282')
HI_C9 = decimal.Decimal('-0.00000199')
HI_FIRST_ADJUSTMENT_THRESHOLD = (decimal.Decimal('80.0'), decimal.Decimal('112.0'), decimal.Decimal('13.0'), )
HI_13 = decimal.Decimal('13')
HI_17 = decimal.Decimal('17')
HI_95 = decimal.Decimal('95')
HI_SECOND_ADJUSTMENT_THRESHOLD = (decimal.Decimal('80.0'), decimal.Decimal('87.0'), decimal.Decimal('85.0'), )
HI_85 = decimal.Decimal('85')
HI_87 = decimal.Decimal('87')

# Wind chill constants used by NOAA/NWS in its wind chill tables
WC_C1 = decimal.Decimal('35.74')
WC_C2 = decimal.Decimal('0.6215')
WC_C3 = decimal.Decimal('35.75')
WC_C4 = decimal.Decimal('0.4275')
WC_V_EXP = decimal.Decimal('0.16')

# Constants used by Davis Instruments for its THW calculations
THW_INDEX_CONSTANT = decimal.Decimal('1.072')

# Constants used by the Australian Bureau of Meteorology for its apparent temperature (THSW) calculations
THSW_0_25 = decimal.Decimal('0.25')
THSW_0_348 = decimal.Decimal('0.348')
THSW_0_70 = decimal.Decimal('0.70')
THSW_4_25 = decimal.Decimal('4.25')
THSW_6_105 = decimal.Decimal('6.105')
THSW_17_27 = decimal.Decimal('17.27')
THSW_237_7 = decimal.Decimal('237.7')

HEAT_INDEX_THRESHOLD = decimal.Decimal('70.0')  # degrees Fahrenheit
WIND_CHILL_THRESHOLD = decimal.Decimal('40.0')  # degrees Fahrenheit
DEGREE_DAYS_THRESHOLD = decimal.Decimal('65.0')  # degrees Fahrenheit


def _as_decimal(value):
	"""
	Converts the value to a `Decimal` if it is not already, or returns the existing value if it is a `Decimal`, or
	returns `Decimal(0)` if the existing value is `None`.
	:param value: The value to cast/convert
	:type value: int | long | decimal.Decimal | NoneType
	:return: The value as a `Decimal`
	:rtype: decimal.Decimal
	"""
	return value if isinstance(value, decimal.Decimal) else decimal.Decimal(value or '0')


def convert_fahrenheit_to_kelvin(temperature):
	"""
	Converts the temperature from degrees Fahrenheit to Kelvin.
	:param temperature: The value to convert, which must be in degrees Fahrenheit
	:type temperature: int | long | decimal.Decimal
	:return: The temperature in Kelvin to three decimal places
	:rtype: decimal.Decimal
	"""
	return ((temperature + KELVIN_CONSTANT) * FIVE_NINTHS).quantize(ONE_THOUSANDTH)


def convert_kelvin_to_fahrenheit(temperature):
	"""
	Converts the temperature from Kelvin to degrees Fahrenheit.
	:param temperature: The value to convert, which must be in Kelvin
	:type temperature: int | long | decimal.Decimal
	:return: The temperature in degrees Fahrenheit to three decimal places
	:rtype: decimal.Decimal
	"""
	return ((temperature * NINE_FIFTHS) - KELVIN_CONSTANT).quantize(ONE_THOUSANDTH)


def convert_fahrenheit_to_celsius(temperature):
	"""
	Converts the temperature from degrees Fahrenheit to degrees Celsius.
	:param temperature: The value to convert, which must be in degrees Fahrenheit
	:type temperature: int | long | decimal.Decimal
	:return: The temperature in degrees Celsius to three decimal places
	:rtype: decimal.Decimal
	"""
	return ((temperature - CELSIUS_CONSTANT) * FIVE_NINTHS).quantize(ONE_THOUSANDTH)


def convert_celsius_to_fahrenheit(temperature):
	"""
	Converts the temperature from degrees Celsius to degrees Fahrenheit.
	:param temperature: The value to convert, which must be in degrees Celsius
	:type temperature: int | long | decimal.Decimal
	:return: The temperature in degrees Fahrenheit to three decimal places
	:rtype: decimal.Decimal
	"""
	return ((temperature * NINE_FIFTHS) + CELSIUS_CONSTANT).quantize(ONE_THOUSANDTH)


def convert_inches_of_mercury_to_kilopascals(barometric_pressure):
	"""
	Converts pressure measurements from inches of mercury (inHg) to kilopascals (kPa).
	:param barometric_pressure: The value to convert, which must be in inches of mercury
	:type barometric_pressure: int | long | decimal.Decimal
	:return: The pressure in kilopascals to two decimal places
	:rtype: decimal.Decimal
	"""
	return (barometric_pressure / KILOPASCAL_MERCURY_CONSTANT).quantize(ONE_HUNDREDTH)


def convert_inches_of_mercury_to_millibars(barometric_pressure):
	"""
	Converts pressure measurements from inches of mercury (inHg) to millibars (mb/mbar), also known as
	hectopascals (hPa).
	:param barometric_pressure: The value to convert, which must be in inches of mercury
	:type barometric_pressure: int | long | decimal.Decimal
	:return: The pressure in millibars (hectopascals) to two decimal places
	:rtype: decimal.Decimal
	"""
	return (barometric_pressure / MILLIBAR_MERCURY_CONSTANT).quantize(ONE_HUNDREDTH)


def convert_miles_per_hour_to_meters_per_second(wind_speed):
	"""
	Converts speed from miles per hour (MPH) to meters per second (m/s).
	:param wind_speed: The value to convert, which must be in miles per hour
	:type wind_speed: int | long | decimal.Decimal
	:return: The speed in meters per second to five decimal places
	:rtype: decimal.Decimal
	"""
	return (wind_speed * METERS_PER_SECOND_CONSTANT).quantize(METERS_PER_SECOND_CONSTANT)


# noinspection PyPep8Naming
def calculate_wet_bulb_temperature(temperature, relative_humidity, barometric_pressure):
	"""
	Uses the temperature, relative humidity, and barometric pressure to calculate the wet bulb temperature, which is
	"the temperature a parcel of air would have if it were cooled to saturation (100% relative humidity) by the
	evaporation of water into it." A wet bulb temperature above 80F (27C) is considered uncomfortable, while a wet
	bulb temperature above 95F (35C) is deadly, as it is beyond the threshold at which the human body can cool itself
	and starts absorbing heat from the surrounding environment. The citation comes from
	https://en.wikipedia.org/wiki/Wet-bulb_temperature. The algorithm used and its constants are sourced from
	http://www.aprweather.com/pages/calc.htm. In this algorithm:
	Tc is the temperature in degrees Celsius
	RH is the relative humidity percentage
	P is the atmospheric pressure in millibars
	Tdc is the dew point temperature for this algorithm (may not match the output of `calculate_dew_point`)
	E is the vapor pressure
	Tw is the wet bulb temperature in degrees Celsius
	:param temperature: The temperature in degrees Fahrenheit
	:type temperature: int | long | decimal.Decimal
	:param relative_humidity: The relative humidity expressed as a percentage (88.2 instead of 0.882)
	:type relative_humidity: int | long | decimal.Decimal
	:param barometric_pressure: The atmospheric pressure in inches of mercury
	:type barometric_pressure: int | long | decimal.Decimal
	:return: The wet bulb temperature in degrees Fahrenheit to one decimal place
	:rtype: decimal.Decimal
	"""
	Tc = convert_fahrenheit_to_celsius(temperature)
	RH = _as_decimal(relative_humidity)
	P = convert_inches_of_mercury_to_millibars(barometric_pressure)

	Tdc = (
		Tc - (WB_14_55 + WB_0_114 * Tc) * (1 - (ONE_HUNDREDTH * RH)) -
		((WB_2_5 + WB_0_007 * Tc) * (1 - (ONE_HUNDREDTH * RH))) ** 3 -
		(WB_15_9 + WB_0_117 * Tc) * (1 - (ONE_HUNDREDTH * RH)) ** 14
	)
	E = WB_6_11 * 10 ** (WB_7_5 * Tdc / (WB_237_7 + Tdc))
	Tw = (
		(((WB_0_00066 * P) * Tc) + ((4098 * E) / ((Tdc + WB_237_7) ** 2) * Tdc)) /
		((WB_0_00066 * P) + (4098 * E) / ((Tdc + WB_237_7) ** 2))
	)

	return convert_celsius_to_fahrenheit(Tw).quantize(ONE_TENTH)


# noinspection PyPep8Naming
def calculate_dew_point(temperature, relative_humidity):
	"""
	Uses the temperature and relative humidity to calculate the dew point, a measure of atmospheric moisture that is
	the temperature at which dew forms. "It is the temperature to which air must be cooled at constant pressure and
	water content to reach saturation." A dew point greater than 68F is considered high, while 72F is uncomfortable.
	A dew point of 80F or higher can often be deadly for asthma sufferers. In this algorithm:
	Tc is the temperature in degrees Celsius
	RH is the relative humidity percentage
	Tdc is the dew point temperature
	:param temperature: The temperature in degrees Fahrenheit
	:type temperature: int | long | decimal.Decimal
	:param relative_humidity: The relative humidity as a percentage (88.2 instead of 0.882)
	:type relative_humidity: int | long | decimal.Decimal
	:return: The dew point temperature in degrees Fahrenheit to one decimal place
	:rtype: decimal.Decimal
	"""
	Tc = convert_fahrenheit_to_celsius(temperature)
	RH = _as_decimal(relative_humidity)

	Ym = (
		RH / 100 * (
			(DP_B - (Tc / DP_D)) * (Tc / (DP_C + Tc))
		).exp()
	).ln()
	Tdc = (DP_C * Ym) / (DP_B - Ym)

	return convert_celsius_to_fahrenheit(Tdc).quantize(ONE_TENTH)


def _abs(d):
	return max(d, -d)


# noinspection PyPep8Naming
def calculate_heat_index(temperature, relative_humidity):
	"""
	Uses the temperature and relative humidity to calculate the heat index, the purpose of which is to represent a
	"felt-air temperature" close to what a human actually feels given the temperature and humidity. This index does
	not take into account the wind speed or solar radiation, and so is not the most accurate measure of a true
	"feels-like" temperature. For that, see `calculate_thw_index` and `calculate_thsw_index`. The algorithm used and
	its constants are sourced from http://www.wpc.ncep.noaa.gov/html/heatindex_equation.shtml. In this algorithm:
	T is the temperature in degrees Fahrenheit
	RH is the relative humidity percentage
	This function is tested against the NOAA/NWS heat index chart found at
	http://www.nws.noaa.gov/os/heat/heat_index.shtml. It returns `None` if the input temperature is less than 70F.
	Experts disagree as to whether the heat index is applicable between 70F and 80F, but this function returns a heat
	index calculation for those values.
	:param temperature: The temperature in degrees Fahrenheit
	:type temperature: int | long | decimal.Decimal
	:param relative_humidity: The relative humidity as a percentage (88.2 instead of 0.882)
	:type relative_humidity: int | long | decimal.Decimal
	:return: The heat index temperature in degrees Fahrenheit to one decimal place, or `None` if the temperature is
				less than 70F
	:rtype: decimal.Decimal
	"""
	if temperature < HEAT_INDEX_THRESHOLD:
		return None

	T = temperature
	RH = _as_decimal(relative_humidity)

	heat_index = HI_0_5 * (T + HI_61_0 + ((T - HI_68_0) * HI_1_2) + (RH * HI_0_094))
	heat_index = (heat_index + T) / TWO  # This is the average

	if heat_index < HI_SECOND_FORMULA_THRESHOLD:
		return heat_index.quantize(ONE_TENTH, rounding=decimal.ROUND_CEILING)

	heat_index = (
		HI_C1 + (HI_C2 * T) + (HI_C3 * RH) + (HI_C4 * T * RH) + (HI_C5 * T * T) +
		(HI_C6 * RH * RH) + (HI_C7 * T * T * RH) + (HI_C8 * T * RH * RH) + (HI_C9 * T * T * RH * RH)
	)

	if (HI_FIRST_ADJUSTMENT_THRESHOLD[0] <= T <= HI_FIRST_ADJUSTMENT_THRESHOLD[1] and
				RH < HI_FIRST_ADJUSTMENT_THRESHOLD[2]):
		heat_index -= (
			((HI_13 - RH) / FOUR) * ((HI_17 - _abs(T - HI_95)) / HI_17).sqrt()
		)
	elif (HI_SECOND_ADJUSTMENT_THRESHOLD[0] <= T <= HI_SECOND_ADJUSTMENT_THRESHOLD[1] and
							RH > HI_SECOND_ADJUSTMENT_THRESHOLD[2]):
		heat_index += (
			((RH - HI_85) / TEN) * ((HI_87 - T) / FIVE)
		)

	return heat_index.quantize(ONE_TENTH, rounding=decimal.ROUND_CEILING)


# noinspection PyPep8Naming
def calculate_wind_chill(temperature, wind_speed):
	"""
	Uses the air temperature and wind speed to calculate the wind chill, the purpose of which is to represent a
	"felt-air temperature" close to what a human actually feels given the temperature and wind speed. This index does
	not take into account the humidity or solar radiation, and so is not the most accurate measure of a true
	"feels-like" temperature. For that, see `calculate_thw_index` and `calculate_thsw_index`. The algorithm used and
	its constants are sourced from the chart at http://www.srh.noaa.gov/ssd/html/windchil.htm, and the function is
	tested against the same chart. In this algorithm:
	T is the temperature in degrees Fahrenheit
	WS is the wind speed in miles per hour
	This function returns `None` if the input temperature is above 40F.
	:param temperature: The temperature in degrees Fahrenheit
	:type temperature: int | long | decimal.Decimal
	:param wind_speed: The wind speed in miles per hour
	:type wind_speed: int | long | decimal.Decimal
	:return: The wind chill temperature in degrees Fahrenheit to one decimal place, or `None` if the temperature is
				higher than 40F
	:rtype: decimal.Decimal
	"""
	if temperature > WIND_CHILL_THRESHOLD:
		return None

	T = temperature
	WS = _as_decimal(wind_speed)

	if WS == ZERO:  # No wind results in no chill, so skip it
		return T

	V = WS ** WC_V_EXP
	wind_chill = (
		WC_C1 + (WC_C2 * T) - (WC_C3 * V) + (WC_C4 * T * V)
	).quantize(ONE_TENTH, rounding=decimal.ROUND_FLOOR)

	return T if wind_chill > T else wind_chill


# noinspection PyPep8Naming
def calculate_thw_index(temperature, relative_humidity, wind_speed):
	"""
	Uses the air temperature, relative humidity, and wind speed (THW = temperature-humidity-wind) to calculate a
	potentially more accurate "felt-air temperature." This is not as accurate, however, as the THSW index, which
	can only be calculated when solar radiation information is available. It uses `calculate_heat_index` and then
	applies additional calculations to it using the wind speed. As such, it returns `None` for input temperatures below
	70 degrees Fahrenheit. The additional calculations come from web forums rumored to contain the proprietary
	Davis Instruments THW index formulas.
	hi is the heat index as calculated by `calculate_heat_index`
	WS is the wind speed in miles per hour
	:param temperature: The temperature in degrees Fahrenheit
	:type temperature: int | long | decimal.Decimal
	:param relative_humidity: The relative humidity as a percentage (88.2 instead of 0.882)
	:type relative_humidity: int | long | decimal.Decimal
	:param wind_speed: The wind speed in miles per hour
	:type wind_speed: int | long | decimal.Decimal
	:return: The THW index temperature in degrees Fahrenheit to one decimal place, or `None` if the temperature is
				less than 70F
	:rtype: decimal.Decimal
	"""
	hi = calculate_heat_index(temperature, relative_humidity)
	WS = _as_decimal(wind_speed)

	if not hi:
		return None
	return hi - (THW_INDEX_CONSTANT * WS).quantize(ONE_TENTH, rounding=decimal.ROUND_CEILING)


# noinspection PyPep8Naming
def calculate_thsw_index(temperature, relative_humidity, solar_radiation, wind_speed):
	"""
	Uses the air temperature, relative humidity, solar radiation, and wind speed (THSW = temperature-humidity-sun-wind)
	to calculate a potentially more accurate "felt-air temperature." Given that it uses all the variables that affect
	how the human body perceives temperature, it is likely the most accurate measure of a true "feels like" temperature.
	It is applied to all temperatures, high and low. Though named to mimic the THSW index from Davis Instruments, the
	algorithm comes from the Australian Bureau of Meteorology
	(http://www.bom.gov.au/info/thermal_stress/#atapproximation). The calculations for Q1, Q2, and Q3 come from
	http://reg.bom.gov.au/amm/docs/1994/steadman.pdf. In this algorithm:
	Tc is the temperature in degrees Celsius
	RH is the relative humidity percentage
	QD is the direct thermal radiation in watts absorbed per square meter of surface area
	Qd is the diffuse thermal radiation in watts absorbed per square meter of surface area
	Q1 is the thermal radiation in watts absorbed per square meter of surface area as measured by a pyranometer;
		it represents "global radiation" (QD + Qd)
	Q2 is the direct and diffuse radiation in watts absorbed per square meter of surface on the human body
	Q3 is the ground-reflected radiation in watts absorbed per square meter of surface on the human body
	Q is total thermal radiation that affects apparent temperature
	WS is the wind speed in meters per second
	E is the water vapor pressure
	Thsw is the THSW index temperature
	:param temperature: The temperature in degrees Fahrenheit
	:type temperature: int | long | decimal.Decimal
	:param relative_humidity: The relative humidity as a percentage (88.2 instead of 0.882)
	:type relative_humidity: int | long | decimal.Decimal
	:param solar_radiation: The absorbed solar radiation in watts per square meter
	:type solar_radiation: int | long | decimal.Decimal
	:param wind_speed: The wind speed in miles per hour
	:type wind_speed: int | long | decimal.Decimal
	:return: The THSW index temperature in degrees Fahrenheit to one decimal place
	:rtype: decimal.Decimal
	"""
	Tc = convert_fahrenheit_to_celsius(temperature)
	RH = _as_decimal(relative_humidity)
	Q1 = _as_decimal(solar_radiation)
	WS = convert_miles_per_hour_to_meters_per_second(_as_decimal(wind_speed))

	# TODO We know Q1 (input variable), and we know that Q1 = QD + Qd. But we need Qd. To do that, we need to figure
	# TODO out how much of Q1 is Qd. So we calculate what QDe and Qde (e = expected) should be based on the angle of
	# TODO the sun in the sky using radiation tables. Given that Q1e = QDe + Qde, we can solve for x in xQ1e = Q1
	# TODO and apply x to QDe and Qde to determine the most likely QD and Qd. For now, we'll use a statistical average
	# TODO to determine QD and Qd, given that Qd is usually 25% of Q1 in Tennessee in summer.

	Qd = Q1 * THSW_0_25
	# QDe, Qde = get_expected_solar_radiation(latitude, longitude, timestamp)
	# QD = Q1 - Qd

	Q2 = Qd / 7
	Q3 = Q1 / 28
	Q = Q2 + Q3

	E = RH / 100 * THSW_6_105 * (THSW_17_27 * Tc / (THSW_237_7 + Tc)).exp()
	Thsw = Tc + (THSW_0_348 * E) - (THSW_0_70 * WS) + ((THSW_0_70 * Q) / (WS + 10)) - THSW_4_25

	return convert_celsius_to_fahrenheit(Thsw).quantize(ONE_TENTH)


def calculate_cooling_degree_days(average_temperature):
	"""
	Calculates the cooling degree days for a given day based on its average temperature. The result of this is only
	valid for a daily average temperature. Any application of this to a weekly, monthly, or yearly average temperature
	will yield incorrect results. It must be calculated daily and summed over weekly, monthly, or yearly periods.
	:param average_temperature: The average daily temperature in degrees Fahrenheit
	:type average_temperature: int | long | decimal.Decimal
	:return: The cooling degree days, or `None` if the average temperature was less than or equal to 65F
	"""
	if average_temperature <= DEGREE_DAYS_THRESHOLD:
		return None
	return average_temperature - DEGREE_DAYS_THRESHOLD


def calculate_heating_degree_days(average_temperature):
	"""
	Calculates the heating degree days for a given day based on its average temperature. The result of this is only
	valid for a daily average temperature. Any application of this to a weekly, monthly, or yearly average temperature
	will yield incorrect results. It must be calculated daily and summed over weekly, monthly, or yearly periods.
	:param average_temperature: The average daily temperature in degrees Fahrenheit
	:type average_temperature: int | long | decimal.Decimal
	:return: The heating degree days, or `None` if the average temperature was greater than or equal to 65F
	"""
	if average_temperature >= DEGREE_DAYS_THRESHOLD:
		return None
	return DEGREE_DAYS_THRESHOLD - average_temperature


def calculate_10_minute_wind_average(records):
	"""
	Calculates the highest 10-minute wind average over the course of a day's wind samples. It is only applicable if
	all the wind samples represent time frames of 10 minutes or less. If the archive interval or sample rate of the
	weather instrument is longer than 10 minutes for any wind sample, this function returns `None`.
	The input record format should be a list of lists, a list of tuples, a tuple of lists, or a tuple of tuples, in
	the following format:
	(
		(wind_speed, wind_direction, timestamp_station, minutes_covered, ),
		(wind_speed, wind_direction, timestamp_station, minutes_covered, ),
		(wind_speed, wind_direction, timestamp_station, minutes_covered, ),
		...,
	)
	The wind speed may be any value that can have arithmetic applied to it (specifically, any value that can be summed
	and divided). The wind direction may be any hashable value at all (string, number, etc.), as it is not used or
	manipulated, other than being grouped along with the other values to be returned properly. The timestamp of the
	station must be a `datetime.datetime` and the minutes covered must be an integer, long, or `decimal.Decimal`, as
	they are used together to determine how to weight each record against other records. Minutes covered must be a
	whole number if it is 1 or more, but it may be less than 1 for rapid sample rates (for example, a 2.5-second sample
	rate would yield a minutes-covered value of 0.04166666666667).
	This returns a tuple of (10mwaS, 10mwaD, 10mwaTs, 10mwaTe), where:
		- 10mwaS is the highest 10-minute wind average speed in the same unit as was input
		- 10mwaD is the statistical mode of all the recorded wind directions during the high 10-minute wind average
			period
		- 10mwaTs is start time of the high 10-minute wind average period: in more technical terms, it is the result
			of the following:
			- If the number of minutes covered is 1, it is the value of the timestamp from the first record in the high
				10-minute wind average period
			- If the number of minutes covered is greater than 1, it is 1 less than minutes covered subtracted from
				the value of the timestamp from the first record in the high 10-minute wind average period
		- 10mwaTe is the end time of the high 10-minute wind average period
	The logic behind the start timestamp seems complex, but it's the result of the fact that a single record can
	represent multiple minutes of wind samples, and the provided timestamp only represents the end of the sampling
	period. So, if the number of minutes covered is 10, the timestamp is the end of the 10th minute. The values
	returned, therefore, end up being the timestamp for the end of the 1st minute (the start) and the timestamp for
	the end of the 10th minute (the end). If the number of minutes covered is 1, the timestamp is the end of just that
	minute, but 10 records make up the 10-minute period. So the values returned are the timestamp for the end of the
	1st 1-minute period record (the start) and the timestamp for the end of the 10th 1-minute period record (the end).
	In both cases, all the caller must do to determine the true start time (as opposed to the end timestamp for
	the start minute) is to subtract 1 minute from it.
	:param records: The wind sample records in the above described format
	:type records: list | tuple
	:return: A tuple in the above described format
	:rtype: tuple
	"""
	speed_queue = collections.deque(maxlen=10)
	direction_queue = collections.deque(maxlen=10)
	timestamp_queue = collections.deque(maxlen=10)
	current_max = ZERO
	current_direction_list = []
	current_timestamp_list = []

	for (wind_speed, wind_speed_direction, timestamp_station, minutes_covered, ) in records:
		minutes_covered = int(minutes_covered)
		if minutes_covered > 10:
			# We can't calculate this unless all the records cover 10 or fewer minutes
			return None, None, None, None

		wind_speed = _as_decimal(wind_speed)

		# We want each record to be present in the queue the same number of times as minutes it spans
		# So if a record spans 5 minutes, it counts as 5 items in the 10-minute queue
		speed_queue.extend([wind_speed] * minutes_covered)
		direction_queue.extend([wind_speed_direction] * minutes_covered)

		# The timestamp is special, because we need to do some math with it
		if minutes_covered == 1:
			timestamp_queue.append(timestamp_station)
		else:
			# The timestamp represents the end of the time span
			timestamp_queue.extend(
				[timestamp_station - datetime.timedelta(minutes=i) for i in range(minutes_covered - 1, -1, -1)]
			)

		if len(speed_queue) == 10:
			# This is the rolling average of the last 10 minutes
			average = sum(speed_queue) / 10
			if average > current_max:
				current_max = average
				current_direction_list = list(direction_queue)
				current_timestamp_list = list(timestamp_queue)

	if current_max > ZERO:
		wind_speed_high_10_minute_average = current_max

		wind_speed_high_10_minute_average_direction = None
		wind_speed_high_10_minute_average_start = None
		wind_speed_high_10_minute_average_end = None

		if current_direction_list:
			count = collections.Counter(current_direction_list)
			wind_speed_high_10_minute_average_direction = count.most_common()[0][0]

		if current_timestamp_list:
			wind_speed_high_10_minute_average_start = current_timestamp_list[0]
			wind_speed_high_10_minute_average_end = current_timestamp_list[-1]

		return (
			wind_speed_high_10_minute_average,
			wind_speed_high_10_minute_average_direction,
			wind_speed_high_10_minute_average_start,
			wind_speed_high_10_minute_average_end,
		)

	return None, None, None, None


def _append_to_list(l, v):
	if v:
		l.append(v)


def calculate_all_record_values(record):
	arguments = {}

	wind_speed = _as_decimal(record.get('wind_speed'))
	wind_speed_high = record.get('wind_speed_high')
	humidity_outside = record.get('humidity_outside')
	humidity_inside = record.get('humidity_inside')
	barometric_pressure = record.get('barometric_pressure')
	temperature_outside = record.get('temperature_outside')
	temperature_outside_low = record.get('temperature_outside_low')
	temperature_outside_high = record.get('temperature_outside_high')
	temperature_inside = record.get('temperature_inside')
	solar_radiation = record.get('solar_radiation')
	solar_radiation_high = record.get('solar_radiation_high')

	if wind_speed:
		ws_mpm = wind_speed / 60
		distance = ws_mpm * record['minutes_covered']
		arguments['wind_run_distance_total'] = distance

	if humidity_outside and barometric_pressure:
		if temperature_outside:
			a = calculate_wet_bulb_temperature(temperature_outside, humidity_outside, barometric_pressure)
			if a:
				arguments['temperature_wet_bulb'] = a
		if temperature_outside_low:
			a = calculate_wet_bulb_temperature(temperature_outside_low, humidity_outside, barometric_pressure)
			if a:
				arguments['temperature_wet_bulb_low'] = a
		if temperature_outside_high:
			a = calculate_wet_bulb_temperature(temperature_outside_high, humidity_outside, barometric_pressure)
			if a:
				arguments['temperature_wet_bulb_high'] = a

	if humidity_outside:
		a = []
		b = []
		if temperature_outside:
			_append_to_list(a, calculate_dew_point(temperature_outside, humidity_outside))
			_append_to_list(b, calculate_heat_index(temperature_outside, humidity_outside))
		if temperature_outside_low:
			_append_to_list(a, calculate_dew_point(temperature_outside_low, humidity_outside))
			_append_to_list(b, calculate_heat_index(temperature_outside_low, humidity_outside))
		if temperature_outside_high:
			_append_to_list(a, calculate_dew_point(temperature_outside_high, humidity_outside))
			_append_to_list(b, calculate_heat_index(temperature_outside_high, humidity_outside))
		if a:
			arguments['dew_point_outside'] = a[0]
			arguments['dew_point_outside_low'] = min(a)
			arguments['dew_point_outside_high'] = max(a)
		if b:
			arguments['heat_index_outside'] = b[0]
			arguments['heat_index_outside_low'] = min(b)
			arguments['heat_index_outside_high'] = max(b)

	if humidity_inside and temperature_inside:
		a = calculate_dew_point(temperature_inside, humidity_inside)
		b = calculate_heat_index(temperature_inside, humidity_inside)
		if a:
			arguments['dew_point_inside'] = a
		if b:
			arguments['heat_index_inside'] = b

	if (wind_speed or wind_speed_high) and (temperature_outside or temperature_outside_high or temperature_outside_low):
		a = []
		if wind_speed and temperature_outside:
			_append_to_list(a, calculate_wind_chill(temperature_outside, wind_speed))
		if wind_speed and temperature_outside_high:
			_append_to_list(a, calculate_wind_chill(temperature_outside_high, wind_speed))
		if wind_speed and temperature_outside_low:
			_append_to_list(a, calculate_wind_chill(temperature_outside_low, wind_speed))
		if wind_speed_high and temperature_outside:
			_append_to_list(a, calculate_wind_chill(temperature_outside, wind_speed_high))
		if wind_speed_high and temperature_outside_high:
			_append_to_list(a, calculate_wind_chill(temperature_outside_high, wind_speed_high))
		if wind_speed_high and temperature_outside_low:
			_append_to_list(a, calculate_wind_chill(temperature_outside_low, wind_speed_high))
		if a:
			arguments['wind_chill'] = a[0]
			arguments['wind_chill_low'] = min(a)
			arguments['wind_chill_high'] = max(a)

	if humidity_outside and (temperature_outside or temperature_outside_high or temperature_outside_low):
		ws = wind_speed if wind_speed else 0
		wsh = wind_speed_high if wind_speed_high else 0

		a = []
		if temperature_outside:
			_append_to_list(a, calculate_thw_index(temperature_outside, humidity_outside, ws))
			_append_to_list(a, calculate_thw_index(temperature_outside, humidity_outside, wsh))
		if temperature_outside_high:
			_append_to_list(a, calculate_thw_index(temperature_outside_high, humidity_outside, ws))
			_append_to_list(a, calculate_thw_index(temperature_outside_high, humidity_outside, wsh))
		if temperature_outside_low:
			_append_to_list(a, calculate_thw_index(temperature_outside_low, humidity_outside, ws))
			_append_to_list(a, calculate_thw_index(temperature_outside_low, humidity_outside, wsh))
		if a:
			arguments['thw_index'] = a[0]
			arguments['thw_index_low'] = min(a)
			arguments['thw_index_high'] = max(a)

		if solar_radiation or solar_radiation_high:
			a = []
			if temperature_outside and solar_radiation:
				_append_to_list(a, calculate_thsw_index(temperature_outside, humidity_outside, solar_radiation, ws))
				_append_to_list(a, calculate_thsw_index(temperature_outside, humidity_outside, solar_radiation, wsh))
			if temperature_outside_high and solar_radiation:
				_append_to_list(
					a,
					calculate_thsw_index(temperature_outside_high, humidity_outside, solar_radiation, ws),
				)
				_append_to_list(
					a,
					calculate_thsw_index(temperature_outside_high, humidity_outside, solar_radiation, wsh),
				)
			if temperature_outside_low and solar_radiation:
				_append_to_list(
					a,
					calculate_thsw_index(temperature_outside_low, humidity_outside, solar_radiation, ws),
				)
				_append_to_list(
					a,
					calculate_thsw_index(temperature_outside_low, humidity_outside, solar_radiation, wsh),
				)
			if temperature_outside and solar_radiation_high:
				_append_to_list(
					a,
					calculate_thsw_index(temperature_outside, humidity_outside, solar_radiation_high, ws),
				)
				_append_to_list(
					a,
					calculate_thsw_index(temperature_outside, humidity_outside, solar_radiation_high, wsh),
				)
			if temperature_outside_high and solar_radiation_high:
				_append_to_list(
					a,
					calculate_thsw_index(temperature_outside_high, humidity_outside, solar_radiation_high, ws),
				)
				_append_to_list(
					a,
					calculate_thsw_index(temperature_outside_high, humidity_outside, solar_radiation_high, wsh),
				)
			if temperature_outside_low and solar_radiation_high:
				_append_to_list(
					a,
					calculate_thsw_index(temperature_outside_low, humidity_outside, solar_radiation_high, ws),
				)
				_append_to_list(
					a,
					calculate_thsw_index(temperature_outside_low, humidity_outside, solar_radiation_high, wsh),
				)
			if a:
				arguments['thsw_index'] = a[0]
				arguments['thsw_index_low'] = min(a)
				arguments['thsw_index_high'] = max(a)

	return arguments

##### Import Class #####
class Importer(object):
	FILE_EXTENSION = '.wlk'
	FILE_EXTENSION_LENGTH = len(FILE_EXTENSION)
	EXPECTED_FILE_NAME_LENGTH = 7

	def __init__(self, file_name):
		if not file_name:
			raise ValueError('file_name')
		if file_name[-self.FILE_EXTENSION_LENGTH:] != self.FILE_EXTENSION:
			raise ValueError('file_name')

		self.file_name = file_name

		start_index = -(self.FILE_EXTENSION_LENGTH + self.EXPECTED_FILE_NAME_LENGTH)
		year, month = file_name[start_index:][:self.EXPECTED_FILE_NAME_LENGTH].split('-')

		self.year = int(year)
		self.month = int(month)

		self.header = None
		self.daily_summaries = None
		self.records = None
		self.daily_records = None

	def import_data(self):
		with open(self.file_name, 'rb') as file_handle:
			self.header = Header.load_from_wlk(file_handle)
			self.daily_summaries = {}
			self.records = []
			self.daily_records = collections.defaultdict(list)

			for day, day_index in enumerate(self.header.day_indexes):
				if day > 0:
					for r in range(0, day_index.record_count - 1):
						if r == 0:
							self.daily_summaries[day] = DailySummary.load_from_wlk(
								file_handle,
								self.year,
								self.month,
								day,
							)
						else:
							record = ArchiveIntervalRecord.load_from_wlk(file_handle, self.year, self.month, day)
							self.records.append(record)
							self.daily_records[day].append(record)
		return self

##### JSON #####
def json_repr(obj):
	#Source: https://stackoverflow.com/questions/2343535/easiest-way-to-serialize-a-simple-class-object-with-simplejson
  """Represent instance of a class as JSON.
  Arguments:
  obj -- any object
  Return:
  String that reprent JSON-encoded object.
  """
  def serialize(obj):
    """Recursively walk object's hierarchy."""
    if isinstance(obj, (bool, int, float, str)):
      return obj
    elif isinstance(obj, dict):
      obj = obj.copy()
      for key in obj:
        obj[key] = serialize(obj[key])
      return obj
    elif isinstance(obj, list):
      return [serialize(item) for item in obj]
    elif isinstance(obj, tuple):
      return tuple(serialize([item for item in obj]))
    elif hasattr(obj, '__dict__'):
      return serialize(obj.__dict__)
    else:
      return repr(obj) # Don't know how to handle, convert to string
  return json.dumps(serialize(obj))

WeatherFile = Importer(r"D:\Temp\WeatherStation\2020-04.wlk").import_data()

print(json_repr(WeatherFile))