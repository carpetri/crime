import os
import sys
from datetime import datetime
import numpy as np
import pandas as pd
import cStringIO
import csv
#Spark 
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import Row, Column
from pyspark.sql.functions import *
from pyspark.sql.functions import udf

# if not sc:
sc= SparkContext()
# if not sqlContext:
sqlContext = SQLContext(sc)

#
schema = StructType([
	# StructField("usaf",StringType(),True),
	# StructField("wban",StringType(),True),
	StructField("station",StringType(),True),
	StructField("time",TimestampType(),True),
	# StructField("year",StringType(),True),
	# StructField("month",StringType(),True),
	# StructField("day",StringType(),True),
	# StructField("hour",StringType(),True),
	StructField("air_temp_celsius",DoubleType(),True),
	StructField("dew_point_temp_celsius",DoubleType(),True),
	StructField("sea_level_pressure",DoubleType(),True),
	StructField("wind_direction",DoubleType(),True),
	StructField("wind_speed_km_hr",DoubleType(),True),
	StructField("sky_condition_total_coverage_code",StringType(),True),
	StructField("liquid_precipitation_mm_one_hour",DoubleType(),True),
	StructField("liquid_precipitation_mm_six_hours",DoubleType(),True),
	])



#Get USER
user = os. environ['USER']
if user not in ['cpa253','vaa238','vm1370']:
	user = 'cpa253'

weather_folder= "/user/%s/rbda/crime/data/weather" % user

def parse_csv(line):
    """ Function to parse csv"""
    f = cStringIO.StringIO(line)
    readCSV = csv.reader(f, delimiter=',')
    return [row for row in readCSV]

def clean_empty(x):
	if x == '':
		return None
	else:
		return x

def get_station_name(x):
	station = None
	if x == '725053' or x == '725033':
		station = 'Central Park'

	if x == '744860':
		station = 'JFK'

	if x == '725030':
		station = 'La Guardia' 
	return station 
	
def to_timestamp(yy, mm, dd, hh ):
	return datetime.strptime("%s%s%s %s" % (yy,mm,dd,hh), '%Y%m%d %H') 
	
def to_double(x):
	try:
		return float(x)
	except ValueError:
		return None


def parse_line(l):
	r = Row(
		'station',
		#"usaf", #l[0]
		#"wban", #l[1]
		# "year", #l[2]
		# "month", #l[3]
		# "day", #l[3]
		# "hour", #l[4]
		"time",
		"air_temp_celsius", #l[5]
		"dew_point_temp_celsius", #l[6]
		"sea_level_pressure", #l[7]
		"wind_direction", #l[8]
		"wind_speed_km_hr", #l[9]
		"sky_condition_total_coverage_code", #l[10]
		"liquid_precipitation_mm_one_hour", #l[11]
		"liquid_precipitation_mm_six_hours", #l[12]
		)

	return r(
	  	get_station_name(l[0]),
		to_timestamp(l[2],l[3],l[4],l[5]),
		# l[2] ,#"year",
		# l[3] ,#"month",
		# l[4] ,#"day",
		# l[5] ,#"hour",
		to_double(l[6]) ,#"air_temp_celsius",
		to_double(l[7]) ,#"dew_point_temp_celsius",
		to_double(l[8]),#"sea_level_pressure",
		to_double(l[9]),#"wind_direction",
		to_double(l[10]) ,#"wind_speed_km_hr",
		l[11], #"sky_condition_total_coverage_code", 
		to_double(l[12]), #"liquid_precipitation_mm_one_hour", 
		to_double(l[13]), #"liquid_precipitation_mm_six_hours", 
	  )


dat = sc.textFile(weather_folder).\
flatMap(parse_csv).\
filter(lambda x: x[0]!= 'usaf').\
map(parse_line).\
toDF(schema)

station_dates = dat.select('station',year('time')).distinct().toPandas()

station_dates.sort_values(by=['station','year(time)'])

output_folder = '/user/%s/rbda/crime/data/weather_clean' %(user)

print 'Saving to hdfs://%s' % output_folder
dat.write.mode('overwrite').save(output_folder)



