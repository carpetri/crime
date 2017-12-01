import os
import sys
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql import Row, Column
from pyspark.sql.functions import *
from datetime import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import udf

user = os. environ['USER']
if user not in ['cpa253','vaa238','vm1370']:
	user = 'cpa253'

if not sc:
	sc = SparkContext()
if not sqlContext:
	sqlContext = SQLContext(sc)

weather = sqlContext.read.parquet('/user/%s/rbda/crime/data/weather_clean' %(user) ).filter("year(time) >= 2009")


def get_station(lon,lat):
	s_coords = pd.DataFrame({
		'station':  [
			"Central Park",
			"La Guardia",
			"JFK",
			]
		,
		'lat': [ 
			40.782483,
			40.776212, 
			40.640773, 
			], 
		'lon':[
			-73.965816,
			-73.874009,
			-73.779180,
			]
	})

	if not lon:
		return None
	if not lon:
		return 

	s_coords['dist'] =  (s_coords.lon - lon)**2 + (s_coords.lat - lat)**2
	ind = s_coords['dist'].idxmin(axis=0)
	out = s_coords.station[ind]
	return out

get_station_udf = udf(  get_station )

file_name = '/user/%s/rbda/crime/data/crime_clean' %(user) 
		
df = sqlContext.read.parquet(file_name)
		