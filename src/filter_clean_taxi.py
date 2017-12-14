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


try:
	sc = SparkContext()
	sqlContext = SQLContext(sc)
except:
	print('SC availave')


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
 
station_map = sqlContext.read.format("com.databricks.spark.csv").\
	option('header','true').\
	load('rbda/crime/data/map_zones_to_weather_stations.csv')

for y in range(2009,2016):
	for m in range(1,13):
		file_name = '/user/%s/rbda/crime/data/taxi_data_clean/yellow/year=%d/month=%02d' %(user,y,m) 
		
		df = sqlContext.read.parquet(file_name)
		
		df = df.withColumn('fare_amount', abs(df.fare_amount))

		df = df.withColumn('extra', abs(df.extra))

		df = df.withColumn('mta_tax', abs(df.mta_tax))

		df = df.withColumn('tip_amount',abs(df.tip_amount))

		df = df.withColumn('total_amount',abs(df.total_amount))

		df = df.withColumn('tolls_amount',abs(df.tolls_amount))

		df = df.withColumn('improvement_surcharge', 
		        when( year(df.pickup_datetime) < 2015 , 0.0).otherwise( 0.3 ) )
		
		#df.describe(['fare_amount','extra','mta_tax','tip_amount','total_amount']).show()
		
		df = df\
			.filter( "pickup_longitude < -73.0 OR pickup_longitude IS NULL"
			 )\
			.filter( "pickup_longitude > -74.3 OR pickup_longitude IS NULL"
			        )\
			.filter( "pickup_latitude < 41.0  OR pickup_latitude is null")\
			.filter( "pickup_latitude > 40.0 OR pickup_latitude is null" )\
			.filter( "dropoff_longitude < -73.0 OR dropoff_longitude is null")\
			.filter( "dropoff_longitude > -74.3 OR dropoff_longitude is null")\
			.filter( "dropoff_latitude < 41.0 OR dropoff_latitude is null")\
			.filter( "dropoff_latitude > 40.0 OR dropoff_latitude is null")

		df = df\
			.filter( df.trip_distance > 0.0)\
			.filter( df.trip_distance < 100.0)\
			.filter( df.fare_amount < 500 )\
			.filter( df.extra < 100 )\
			.filter( df.mta_tax < 100 )\
			.filter( df.tip_amount < 200 )\
			.filter( df.tolls_amount < 100 )\
			.filter( df.total_amount < 1000 )

		# df.describe(['fare_amount','extra','mta_tax','tip_amount','total_amount']).show()

		df = df.withColumn('station', get_station_udf("pickup_longitude","pickup_latitude") )

		output_folder = '/user/%s/rbda/crime/data/taxi_data_clean_weather/yellow/year=%d/month=%02d' %(user,y,m)
		
		print('Saving to hdfs://%s' % output_folder)
		
		df.write.mode('ignore').save(output_folder)
		

for y in range(2016,2018):
	for m in range(1,13):
		file_name = '/user/%s/rbda/crime/data/taxi_data_clean/yellow/year=%d/month=%02d' %(user,y,m) 
		
		df = sqlContext.read.parquet(file_name)
		
		df = df.withColumn('fare_amount', abs(df.fare_amount))

		df = df.withColumn('extra', abs(df.extra))

		df = df.withColumn('mta_tax', abs(df.mta_tax))

		df = df.withColumn('tip_amount',abs(df.tip_amount))

		df = df.withColumn('total_amount',abs(df.total_amount))

		df = df.withColumn('tolls_amount',abs(df.tolls_amount))

		df = df.withColumn('improvement_surcharge', 
		        when( year(df.pickup_datetime) < 2015 , 0.0).otherwise( 0.3 ) )
		

		df = df\
			.filter( df.trip_distance > 0.0)\
			.filter( df.trip_distance < 100.0)\
			.filter( df.fare_amount < 500 )\
			.filter( df.extra < 100 )\
			.filter( df.mta_tax < 100 )\
			.filter( df.tip_amount < 200 )\
			.filter( df.tolls_amount < 100 )\
			.filter( df.total_amount < 1000 )

		
		df = df.join(station_map,df.pickup_location_id == station_map.taxi_zone_id).drop('taxi_zone_id')


		output_folder = '/user/%s/rbda/crime/data/taxi_data_clean_weather/yellow/year=%d/month=%02d' %(user,y,m)
		
		print('Saving to hdfs://%s' % output_folder)
		
		df.write.mode('ignore').save(output_folder)
		


