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


sc= SparkContext()
# if not sqlContext:
sqlContext = SQLContext(sc)


import geopandas as gpd
# GeoDataFrame creation
poly = gpd.read_file("../data/taxi_zones/taxi_zones_clean.shp")
poly.head()
points = poly.copy()
# change the geometry
points.geometry = points['geometry'].centroid
# same crs
points.crs =poly.crs
points.head()


def get_station_name(x):
	station = None
	if x == '725053' or x == '725033':
		station = 'Central Park'

	if x == '744860':
		station = 'JFK'

	if x == '725030':
		station = 'La Guardia' 
	return station 




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

points['station']= points.geometry.map(get_station_name)

dat  =  sc.parallelize(p)    

user='cpa253'
output_folder = '/user/%s/rbda/crime/data/stations_centroids' %(user)
df.write.mode('ignore').save(output_folder)



