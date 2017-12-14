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
from ggplot import *

from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import GeneralizedLinearRegression



file_name = '/user/cpa253/rbda/crime/data/crime_clean'

crimes = sqlContext.read.parquet(file_name).filter('station is not null')

crimes.groupby('station').count().show()

crimes_per_zone = crimes.groupby('taxi_zone_id').count().sort('count',ascending=False).withColumnRenamed('count', 'n_crimes')

y=2014
taxi_file_name = '/user/cpa253/rbda/crime/data/taxi_data_clean_weather/yellow/year=%d' %(y)

taxis = sqlContext.read.parquet(taxi_file_name).filter('station is not null')

taxis_pickups = taxis.groupby('pickup_location_id').count().sort('count',ascending=False).withColumnRenamed('count', 'n_pickups')

df =  crimes_per_zone.join(taxis_pickups, taxis_pickups.pickup_location_id == crimes_per_zone.taxi_zone_id).select('taxi_zone_id','n_crimes','n_pickups')

df = df.select('n_crimes','n_pickups')

####### JOINING DATA TAXI AND WEATHER (LIQUID PRECIPITATION)
taxis = sqlContext.read.parquet(taxi_file_name).filter('station is not null')

taxis = taxis.withColumn("hourly_date", ((floor(unix_timestamp(taxis.pickup_datetime) / 3600) * 3600).cast("timestamp")) ).withColumnRenamed('station','Station_taxi')

weather = sqlContext.read.parquet('/user/cpa253/rbda/crime/data/weather_clean').filter("year(time) >= 2009")

weather = weather.withColumn("hourly_date", ((floor(unix_timestamp(weather.time) / 3600) * 3600).cast("timestamp")) )

taxis_w = taxis.join(weather, (taxis.Station_taxi == weather.station) & (taxis.hourly_date == weather.hourly_date)).select('pickup_datetime','dropoff_datetime','trip_distance','total_amount','pickup_location_id','dropoff_location_id','station','liquid_precipitation_mm_one_hour')




#_______________

#Question 1
#Is the average number of taxi pickups different in areas that have different levels of crime rates?

crimes_per_zone = crimes.groupby('taxi_zone_id').count().sort('count',ascending=False).withColumnRenamed('count', 'n_crimes')
crimes_per_zone.withColumn('avg', crimes_per_zone.n_crimes / crimes.count())

y=2014
taxi_file_name = '/user/cpa253/rbda/crime/data/taxi_data_clean_weather/yellow/year=%d' %(y)
taxis = sqlContext.read.parquet(taxi_file_name).filter('station is not null')
taxis_pickups = taxis.groupby('pickup_location_id').count().sort('count',ascending=False).withColumnRenamed('count', 'n_pickups')

Final_Table_1 = crimes_per_zone.join(taxis_pickups, taxis_pickups.pickup_location_id == crimes_per_zone.taxi_zone_id).select('taxi_zone_id','n_pickups','n_crimes').withColumn('avg', crimes_per_zone.n_crimes / crimes.count())


#Question 4
#Does the average number of short rides hve a different average number of taxi pickups in areas that have different levels of crime rates compared to the average of long rides?
ShTrips = taxis.where(taxis.trip_distance < 2.0)
Short_Trips = ShTrips.groupby('pickup_location_id').count().withColumnRenamed('count','n_Short_Trips')

LoTrips = taxis.where(taxis.trip_distance > 2.0)
Long_Trips = LoTrips.groupby('pickup_location_id').count().withColumnRenamed('count','n_Long_Trips').withColumnRenamed('pickup_location_id','Location_ID')

Trip_Distance_Table = Long_Trips.join(Short_Trips, Long_Trips.Location_ID == Short_Trips.pickup_location_id).select('Location_ID','n_Short_Trips','n_Long_Trips')

Final_Table_4 = Final_Table_1.join(Trip_Distance_Table, Trip_Distance_Table.Location_ID == Final_Table_1.taxi_zone_id).select('taxi_zone_id','n_pickups','n_Short_Trips','n_Long_Trips','n_crimes')
Final_Table_4 = Final_Table_4.withColumn('Percentage_Short_Trips',Final_Table_4.n_Short_Trips / Final_Table_4.n_pickups * 100).withColumn('Percentage_Long_Trips',Final_Table_4.n_Long_Trips / Final_Table_4.n_pickups * 100).withColumn('Percentage_Crime', crimes_per_zone.n_crimes / crimes.count() * 100) 


#Question 6
#When categorized by the severity of crime, is the average number of taxi pickups different in areas that have different levels of crime rates for a given level of severity in crimes?
Severity = crimes.groupby('taxi_zone_id','offense_level').count().withColumnRenamed('count','n_crime_severity').withColumnRenamed('taxi_zone_id','Location_ID')
Temp_Table_6 = Severity.join(taxis_pickups, taxis_pickups.pickup_location_id == Severity.Location_ID).select('Location_ID','offense_level','n_pickups','n_crime_severity')
Final_Table_6 = Temp_Table_6.join(crimes_per_zone, Temp_Table_6.Location_ID == crimes_per_zone.taxi_zone_id).select('Location_ID','offense_level','n_pickups','n_crime_severity','n_crimes')
Final_Table_6 = Final_Table_6.withColumn('Percentage_Crime_Severity', Final_Table_6.n_crime_severity / Final_Table_6.n_crimes * 100)



#INCOMPLETE / DOES NOT WORK YET

#Question 3
#Is the average number of taxi pickups different in areas that have different levels of crime rates when we compare at times with and without rain (or different weather variables)?
from_pattern = 'yyyy-MM-dd hh:mm:...'
to_pattern = 'yyyy-MM-dd'
weather = weather.withColumn('part_date', from_unixtime(unix_timestamp(weather['time'], from_pattern), to_pattern))
weather = weather.select('part_date','liquid_precipitation_mm_one_hour')

Rain = weather.groupby('part_date').sum('liquid_precipitation_mm_one_hour').withColumnRenamed('count','Rain?')

taxis = taxis.withColumn('part_date_taxi', from_unixtime(unix_timestamp(taxis['pickup_datetime'], from_pattern), to_pattern))
Taxi_Rain = taxis.join(Rain, taxis.part_date_taxi == Rain.part_date).select('part_date','Rain?')



#Question 5
#Does the answer of the previous questions change for special dates such as holidays?
Holidays = ['2014-01-01', '2014-01-02', '2014-04-07', '2014-11-11', '2014-22-11', '2014-23-11', '2014-24-11', '2014-25-11', '2014-25-12', '2014-31-12']
ShTrips1 = taxis.where(taxis.trip_distance < 2.0)
from_pattern = 'yyyy-MM-dd hh:mm:...'
to_pattern = 'yyyy-MM-dd'
ShTrips1 = ShTrips1.withColumn('part_date', from_unixtime(unix_timestamp(ShTrips1['pickup_datetime'], from_pattern), to_pattern))
ShTrips1 = ShTrips1[ShTrips1.part_date.isin(Holidays)]
Short_Trips1 = ShTrips1.groupby('pickup_location_id').count().withColumnRenamed('count','n_Short_Trips')

LoTrips1 = taxis.where(taxis.trip_distance > 2.0)
LoTrips1 = LoTrips1.withColumn('part_date', from_unixtime(unix_timestamp(LoTrips1['pickup_datetime'], from_pattern), to_pattern))
LoTrips1 = LoTrips1[LoTrips1.part_date.isin(Holidays)]
Long_Trips1 = LoTrips1.groupby('pickup_location_id').count().withColumnRenamed('count','n_Long_Trips').withColumnRenamed('pickup_location_id','Location_ID')

Trip_Distance_Table1 = Long_Trips1.join(Short_Trips1, Long_Trips1.Location_ID == Short_Trips1.pickup_location_id).select('Location_ID','n_Short_Trips','n_Long_Trips')

Final_Table_5 = Final_Table_1.join(Trip_Distance_Table1, Trip_Distance_Table1.Location_ID == Final_Table_1.taxi_zone_id).select('taxi_zone_id','n_pickups','n_Short_Trips','n_Long_Trips','n_crimes')
Final_Table_5 = Final_Table_5.withColumn('Percentage_Short_Trips',Final_Table_4.n_Short_Trips / Final_Table_4.n_pickups * 100).withColumn('Percentage_Long_Trips',Final_Table_4.n_Long_Trips / Final_Table_4.n_pickups * 100).withColumn('Average_Crime_Rate', crimes_per_zone.n_crimes / crimes.count()) 









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

