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

#user = os. environ['USER']
#if user not in ['cpa253','vaa238','vm1370']:

#pyspark --num-executors 8 --driver-memory 32g --executor-memory 32g


user = 'cpa253'

y = 2014
m= 1

file_name = '/user/cpa253/rbda/crime/data/taxi_data_clean_weather/yellow/year=%d' %(y)

df = sqlContext.read.option("mergeSchema", "true").parquet(file_name)


taxis = sqlContext.read.parquet(taxi_file_name).filter('station is not null')

taxis = taxis.withColumn("hourly_date", ((floor(unix_timestamp(taxis.pickup_datetime) / 3600) * 3600).cast("timestamp")) )

weather = sqlContext.read.parquet('/user/%s/rbda/crime/data/weather_clean' %(user) ).filter("year(time) >= 2009")

weather = weather.withColumn("hourly_date", ((floor(unix_timestamp(weather.time) / 3600) * 3600).cast("timestamp")) )


taxis_w = taxis.join(weather, (taxis.station == weather.station) & (taxis.hourly_date == weather.hourly_date), 'left_outer')
