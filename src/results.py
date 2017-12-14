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
import numpy as np

from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel

taxi_file_name = '/user/cpa253/rbda/crime/data/taxi_data_clean_weather/yellow'

crime_file_name = '/user/cpa253/rbda/crime/data/crime_clean'

taxis = sqlContext.read.option("mergeSchema", "true").parquet(taxi_file_name)

taxis = taxis.where('year < 2017')

taxis = taxis.withColumn("hourly_date", ((floor(unix_timestamp(taxis.pickup_datetime) / 3600) * 3600).cast("timestamp")) )

weather = sqlContext.read.parquet('/user/cpa253/rbda/crime/data/weather_clean'  ).filter("year(time) >= 2009")

weather = weather.withColumn("hourly_date", ((floor(unix_timestamp(weather.time) / 3600) * 3600).cast("timestamp")) )


taxis_w = taxis.join(weather, (taxis.station == weather.station) & (taxis.hourly_date == weather.hourly_date), 'left_outer').drop(weather.hourly_date).drop(weather.station)

taxis_data = taxis_w.groupby('year','month',
'hourly_date','pickup_location_id','station','payment_type').agg(
avg(taxis_w.total_amount).alias('total_amount_hour'),
avg(taxis_w.passenger_count).alias('passenger_count_hour'),
avg(taxis_w.trip_distance).alias('trip_distance_hour'),
avg(taxis_w.liquid_precipitation_mm_one_hour).alias('rain_hour'), 
count("*").alias('n_pickups_hour')).withColumn("hourly_date", 
 unix_timestamp("hourly_date").cast("string") ).cache()


output_folder = '/user/cpa253/rbda/crime/data/taxi_data_hourly_weather_crime.csv' 
print('Saving to hdfs://%s' % output_folder)

taxis_data.write.\
	mode('overwrite').\
	format("com.databricks.spark.csv").\
	option("header", "true").\
	option("nullValue",'').\
	save(output_folder )


# taxis_data_mod = \
# taxis_data.select("n_pickups_hour","hourly_date","pickup_location_id","total_amount_hour","passenger_count_hour","trip_distance_hour","rain_hour" ).rdd.map(lambda r: LabeledPoint(r[0],  [r[i] for i in range(1,len(r))] )  ).toDF()


# # Define LinearRegression algorithm
# lr = LinearRegressionModel( intercept=True)
# modelA = lr.fit(taxis_data_mod )


# modelB = lr.fit(taxis_data_mod , {lr.regParam:100.0})

