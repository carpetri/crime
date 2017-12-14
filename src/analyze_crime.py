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
import matplotlib
import matplotlib.pyplot as plt
#rcParams['figure.figsize'] = 6, 4
from ggplot import *

from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import GeneralizedLinearRegression

#user = os. environ['USER']
#if user not in ['cpa253','vaa238','vm1370']:

file_name = '/user/cpa253/rbda/crime/data/crime_clean'

crimes = sqlContext.read.parquet(file_name).filter('station is not null')

crimes.groupby('station').count().show()

crimes_per_zone = crimes.groupby('taxi_zone_id','station','offense_level').count().sort('count',ascending=False).withColumnRenamed('count', 'n_crimes')

crimes_per_zone.withColumn('avg', crimes_per_zone.n_crimes / crimes.count() ).show()

y=2014
taxi_file_name = '/user/cpa253/rbda/crime/data/taxi_data_clean_weather/yellow/year=%d' %(y)

taxis = sqlContext.read.parquet(taxi_file_name).filter('station is not null')

taxis_pickups = taxis.groupby('pickup_location_id').count().sort('count',ascending=False).withColumnRenamed('count', 'n_pickups')

df =  crimes_per_zone.join(taxis_pickups,  taxis_pickups.pickup_location_id == crimes_per_zone.taxi_zone_id).select('taxi_zone_id','n_crimes','n_pickups')

df = df.select('n_crimes','n_pickups')

####### JOINING DATA

taxis = sqlContext.read.parquet(taxi_file_name).filter('station is not null')

taxis = taxis.withColumn("hourly_date", ((floor(unix_timestamp(taxis.pickup_datetime) / 3600) * 3600).cast("timestamp")) )

weather = sqlContext.read.parquet('/user/%s/rbda/crime/data/weather_clean' %(user) ).filter("year(time) >= 2009")

weather = weather.withColumn("hourly_date", ((floor(unix_timestamp(weather.time) / 3600) * 3600).cast("timestamp")) )


taxis_w = taxis.join(weather, (taxis.station == weather.station) & (taxis.hourly_date == weather.hourly_date), 'left_outer')



############### MODELING

for it in xrange(1,100)/10

	glr = GeneralizedLinearRegression(family="gaussian", link="identity", maxIter=10, regParam=it)
	# Fit the model
	model = glr.fit(df)

	# Print the coefficients and intercept for generalized linear regression model
	print("Coefficients: " + str(model.coefficients))
	print("Intercept: " + str(model.intercept))

	# Summarize the model over the training set and print out some metrics
	summary = model.summary
	print("Coefficient Standard Errors: " + str(summary.coefficientStandardErrors))
	print("T Values: " + str(summary.tValues))
	print("P Values: " + str(summary.pValues))
	print("Dispersion: " + str(summary.dispersion))
	print("Null Deviance: " + str(summary.nullDeviance))
	print("Residual Degree Of Freedom Null: " + str(summary.residualDegreeOfFreedomNull))
	print("Deviance: " + str(summary.deviance))
	print("Residual Degree Of Freedom: " + str(summary.residualDegreeOfFreedom))
	print("AIC: " + str(summary.aic))
	print("Deviance Residuals: ")
	summary.residuals().show()

