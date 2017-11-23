import os
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import Row, Column
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from datetime import datetime
# from osgeo import *

# from osgeo import ogr
import numpy as np
import pandas as pd


sc= SparkContext()
sqlContext = SQLContext(sc)
# This part is not working in spark yet. We might have to do it after we have the DF.
#read your shapefile
#DOES NOT WORK
# drv    = ogr.GetDriverByName('ESRI Shapefile')
# ds_in  = drv.Open("../data/taxi_zones/taxi_zones_clean.shp")
# lyr_in = ds_in.GetLayer(0)
# geo_ref = lyr_in.GetSpatialRef()
# idx_reg = lyr_in.GetLayerDefn().GetFieldIndex("LocationID")

# def check_zone(lon=-73.991957, lat=40.721567):
# 	"""Checks the taxi zone for a lon,lat"""
# 	drv    = ogr.GetDriverByName('ESRI Shapefile')
# 	ds_in  = drv.Open("../data/taxi_zones/taxi_zones_clean.shp")
# 	lyr_in = ds_in.GetLayer(0)
# 	geo_ref = lyr_in.GetSpatialRef()
# 	idx_reg = lyr_in.GetLayerDefn().GetFieldIndex("LocationID")
# 	pt = ogr.Geometry(ogr.wkbPoint)
# 	pt.SetPoint_2D(0, lon, lat)
# 	lyr_in.SetSpatialFilter(pt)
# for feat_in in lyr_in:
#     print feat_in.GetFieldAsString(idx_reg)
# # 	    if loc:
# # 	    	return loc

# # pickup_longitude=-73.991957
# # pickup_latitude=40.721567

# # 
# #
# # print "%f,%f" % (point.GetX(), point.GetY())



# taxi_shp='../data/taxi_zones/taxi_zones_clean.shp'
# drv    = ogr.GetDriverByName('ESRI Shapefile')
# ds_in  = drv.Open(taxi_shp)
# lyr_in = ds_in.GetLayer(0)
# geo_ref = lyr_in.GetSpatialRef()
# idx_reg = lyr_in.GetLayerDefn().GetFieldIndex("LocationID")
# # dstlayer = dstshp.CreateLayer('0',geom_type=ogr.wkbPolygon)


# # geojson = """{"type":"Point","coordinates":[-73.991957,40.721567]}"""
# # point = ogr.CreateGeometryFromJson(geojson)

# # 	pt.SetPoint_2D(0, lon, lat)
# # 	lyr_in.SetSpatialFilter(pt)

# pt = ogr.Geometry(ogr.wkbPoint)
# pt.SetPoint_2D(0, -73.991957,40.721567)

# # lyr_in.SetSpatialFilter(pt)

# lyr_in.Intersection(pt)

# # for feat_in in lyr_in:
# #     print feat_in.Intersection(pt)


# layer = ogr.Geometry(3)
# intersection = layer.Intersection(pt)



# get_zone_id(pickup_longitude,pickup_longitude)
# check_zone_udf  = udf( lambda x,y: check_zone(x,y) ,StringType())

# zones = pd.read_csv('../data/taxi_zones/taxi_zones_clean.csv',sep=' ',
# 	header=None, names = ['lon','lat','LocationID'])

schema = StructType([
	StructField("vendor_name",StringType(), True),
	StructField("pickup_datetime",TimestampType(), True),
	StructField("dropoff_datetime",TimestampType(), True),
	StructField("passenger_count",IntegerType(), True),
	StructField("trip_distance",DoubleType(), True),
	StructField("pickup_longitude",DoubleType(), True),
	StructField("pickup_latitude",DoubleType(), True),
	StructField("rate_code",StringType(), True),
	StructField("store_and_fwd_flag",StringType(), True),
	StructField("dropoff_longitude",DoubleType(), True),
	StructField("dropoff_latitude",DoubleType(), True),
	StructField("payment_type",StringType(), True),
	StructField("fare_amount",DoubleType(), True),
	StructField("extra",DoubleType(), True),
	StructField("mta_tax",DoubleType(), True),
	StructField("tip_amount",DoubleType(), True),
	StructField("tolls_amount",DoubleType(), True),
	StructField("improvement_surcharge",DoubleType(), True),
	StructField("total_amount",DoubleType(), True),
		
	StructField("pickup_location_id",StringType(), True),
	StructField("dropoff_location_id",StringType(), True),
])

user = os. environ['USER']
if user not in ['cpa253','vaa238','vm1370']:
	user = 'cpa253'

def clean_vendor_name(x):
	if x in ['VTS','CMT','DDS']:
		return x
	vendor_dict= {'1':'CMT','2':'VTS',"3":'DDS'}
	return vendor_dict[x]

def to_date(x):
	return datetime.strptime(x, '%Y-%m-%d %H:%M:%S')

def clean_passenger_count(x):
	out = int(x)
	if out > 9:
		return None
	else:
		return out

def clean_rate_code(x):
	try:
		if int(x) not in xrange(1,7):
			return None
		else:
			rate_code_dict ={
				"1":"Standard rate",
				"2":"JFK",
				"3":"Newark",
				"4":"Nassau or Westchester",
				"5":"Negotiated fare",
				"6":"Group ride",
				}
		return rate_code_dict[x]
	except ValueError:
		return None

def clean_store_flag(x):
	flag_dict= {
	' ': None,
	'':None,
	'*':None,
	'2':None,
	"1":"Stored",
	"Y":"Stored",
	"0":"Not a stored",
	"N":"Not a stored",
	}
	return flag_dict[x]


def clean_payment_type(x):
	# ONLYU AFTER 2009
	pay_dict={
		"1":"Credit card",
		"2":"Cash",
		"3":"No charge",
		"NO CHARGE":"No charge",
		"4":"Dispute",
		"5":"Unknown",
		"6":"Voided trip",
		'Cas':"Cash",
		'CRE':"Credit card",
		'CREDIT':"Credit card",
		'CRD':"Credit card",
		'CAS':"Cash",
		'CASH':"Cash",
		'CSH':"Cash",
		'Dis':"Dispute",
		'DIS':"Dispute",
		"DISPUTE":"Dispute",
		'Cre':"Credit card",
		'No ':"No charge",
		'NO ':"No charge",
		'NOC':"No charge",
		'NA ':"Unknown",
		'UNK':"Unknown",
	}
	return pay_dict[x.upper()]

def to_double(x):
	try:
		return float(x)
	except ValueError:
		return None


def clean_imp_sur(x):
	if y < 2015:
		return 0.0
	else:
		return float(x)

zones_file = '/user/%s/rbda/crime/data/taxi_zones/taxi_zones_clean.csv' % (user)
zones_rdd = sc.textFile(zones_file).\
	map(lambda x: x.split(' ')).\
	map(lambda (lon,lat,LocationID): (1,lon,lat,LocationID))
zones = pd.read_csv('../data/taxi_zones/taxi_zones_clean.csv', sep =' ', names=['lon','lat','LocationID'])

def get_zone_id(lon,lat, pd_data):
	if not lon:
		return None
	if not lon:
		return None
	pd_data['dist'] =  (pd_data.lon - lon)**2 + (pd_data.lat - lat)**2
	ind=pd_data.idxmin(axis=0)['dist']
	out = pd_data.LocationID[ind]
	return str(out)

# get_zone_broad=sc.broadcast(get_zone_id)
zones_broad=sc.broadcast(zones)

# def get_zone_id(lon,lat):
# 	if not lon:
# 		return None
# 	if not lon:
# 		return None
# 	dist = zones.map(lambda (lo, la, LocationID): 
# 	    ( ( float(lo) - lon )**2 +  (float(la) - lat)**2 , LocationID ) ).min(lambda x: x[0])

# 	# for row in zones.itertuples():
# 	# 	point_y = np.array( (row.lon,row.lat) )
# 	# 	dist = np.linalg.norm(point - point_y)
# 	# 	if( dist < min_dist):
# 	# 		min_dist = dist
# 	# 		min_loc = row.LocationID
# 	return dist[1]


# pickup_longitude=-73.991957
# pickup_latitude=40.721567
# get_zone_id(pickup_longitude,pickup_latitude)
# get_zone_id(-73.901408 ,40.906096)
 
# time( get_zone_id(pickup_longitude,pickup_latitude) )

# This is the schem



def to_row(l):
	"""This function filters the rows form the csv that have an incorrect number of columns and returns a Row for spark DataFrame """
	r = Row( 
       	"vendor_name",
		"pickup_datetime",
		"dropoff_datetime",
		"passenger_count",
		"trip_distance",
		"pickup_longitude",
		"pickup_latitude",
		"rate_code",
		"store_and_fwd_flag",
		"dropoff_longitude",
		"dropoff_latitude",
		"payment_type",
		"fare_amount",
		"extra",
		"mta_tax",
		"tip_amount",
		"tolls_amount",
		"improvement_surcharge",
		"total_amount",
		"pickup_location_id",
		"dropoff_location_id",
	    )

	if y <= 2014:
		out = r(
			clean_vendor_name(l[0]), #vendor_name
		    to_date(l[1]), #pickup_datetime 
		    to_date(l[2]), #dropoff_datetime 
		    clean_passenger_count(l[3]), #passenger_count 
		    to_double(l[4]),  #trip_distance 
		    to_double(l[5]),  #pickup_longitude 
		    to_double(l[6]),  #pickup_latitude
		    clean_rate_code(l[7]),  #rate_code
		    clean_store_flag(l[8]),  #store_and_fwd_flag
		    to_double(l[9]),  #dropoff_longitude
		    to_double(l[10]), #dropoff_latitude
		    clean_payment_type(l[11]), #payment_type
		    to_double(l[12]), #fare_amount
		    to_double(l[13]), #extra
		    to_double(l[14]), #mta_tax
		    to_double(l[15]), #tip_amount
		    to_double(l[16]), #tolls_amount
		    0.0, #improvement_surcharge
		    to_double(l[17]), #total_amount
			# None, 
			# None,
			get_zone_id(to_double(l[5]),to_double(l[6]), zones_broad.value ),
			get_zone_id(to_double(l[9]),to_double(l[10]),zones_broad.value),
			)
		return out
	
	if y == 2015:
		out = r(
			clean_vendor_name(l[0]), #vendor_name
		    to_date(l[1]), #pickup_datetime 
		    to_date(l[2]), #dropoff_datetime 
		    clean_passenger_count(l[3]), #passenger_count 
		    to_double(l[4]),  #trip_distance 
		    to_double(l[5]),  #pickup_longitude 
		    to_double(l[6]),  #pickup_latitude
		    clean_rate_code(l[7]),  #rate_code
		    clean_store_flag(l[8]),  #store_and_fwd_flag
		    to_double(l[9]),  #dropoff_longitude
		    to_double(l[10]), #dropoff_latitude
		    clean_payment_type(l[11]), #payment_type
		    to_double(l[12]), #fare_amount
		    to_double(l[13]), #extra
		    to_double(l[14]), #mta_tax
		    to_double(l[15]), #tip_amount
		    to_double(l[16]), #tolls_amount
		    clean_imp_sur(l[17]), #improvement_surcharge
		    to_double(l[18]), #total_amount
			get_zone_id(to_double(l[5]),to_double(l[6]), zones_broad.value ),
			get_zone_id(to_double(l[9]),to_double(l[10]),zones_broad.value),
			)
		return out

	if y == 2016 and m <= 6:
		out = r(
			clean_vendor_name(l[0]), #vendor_name
		    to_date(l[1]), #pickup_datetime 
		    to_date(l[2]), #dropoff_datetime 
		    clean_passenger_count(l[3]), #passenger_count 
		    to_double(l[4]),  #trip_distance 
		    to_double(l[5]),  #pickup_longitude 
		    to_double(l[6]),  #pickup_latitude
		    clean_rate_code(l[7]),  #rate_code
		    clean_store_flag(l[8]),  #store_and_fwd_flag
		    to_double(l[9]),  #dropoff_longitude
		    to_double(l[10]), #dropoff_latitude
		    clean_payment_type(l[11]), #payment_type
		    to_double(l[12]), #fare_amount
		    to_double(l[13]), #extra
		    to_double(l[14]), #mta_tax
		    to_double(l[15]), #tip_amount
		    to_double(l[16]), #tolls_amount
		    clean_imp_sur(l[17]), #improvement_surcharge
		    to_double(l[18]), #total_amount
			get_zone_id(to_double(l[5]),to_double(l[6]), zones_broad.value ),
			get_zone_id(to_double(l[9]),to_double(l[10]),zones_broad.value),
			)
		return out 
	else: 
		out = r(
			clean_vendor_name(l[0]), #vendor_name
		    to_date(l[1]), #pickup_datetime 
		    to_date(l[2]), #dropoff_datetime 
		    clean_passenger_count(l[3]), #passenger_count 
		    to_double(l[4]),  #trip_distance 
		    None,# to_double(l[5]),  #pickup_longitude 
		    None,# to_double(l[6]),  #pickup_latitude
		    clean_rate_code(l[5]),  #rate_code
		    clean_store_flag(l[6]),  #store_and_fwd_flag
		    None,# to_double(l[9]),  #dropoff_longitude
		    None,# to_double(l[10]), #dropoff_latitude
		    clean_payment_type(l[9]), #payment_type
		    to_double(l[10]), #fare_amount
		    to_double(l[11]), #extra
		    to_double(l[12]), #mta_tax
		    to_double(l[13]), #tip_amount
		    to_double(l[14]), #tolls_amount
		    clean_imp_sur(l[15]), #improvement_surcharge
		    to_double(l[16]), #total_amount
		    l[7], #PICK ID
			l[8], #DROP ID
			)
		return out

def filter_rows(l):
	if y < 2015 and len(l)==18:
		return True

	if y == 2015 and len(l)==19:
		return True

	if y == 2016 and m <= 6 and len(l)== 19:
		return True

	if y == 2016 and m > 6 and len(l)== 19:
		return True

	if y == 2017 and len(l)== 17:
		return True

	return False

# udf_get_zone_id = udf(lambda x, y: get_zone_id(x,y) , StringType())

## Clean 2009 - 2016
for y in xrange(2009,2017):
	for m in xrange(1,13):
		
		# Get the file
		file_name = '/user/%s/rbda/crime/data/taxi_data/yellow/yellow_tripdata_%d-%02d.csv' % (user,y,m)

		print "Cleaning file:\n\t%s" % file_name

		# Parse the csv by ',' and get a Row object
		dat = sc.textFile(file_name).\
		map(lambda l: l.split(",")).\
		filter(filter_rows).\
		map( lambda l: to_row(l) )

		# Create a DF with the schema.
		df = sqlContext.createDataFrame(dat,schema)
		
		# Output to save as partition table
		output_folder = '/user/%s/rbda/crime/data/taxi_data_clean/yellow/year=%d/month=%02d' %(user,y,m)

		print 'Saving to hdfs://%s' % output_folder
		df.write.mode('ignore').save(output_folder)

## Clean current year's data
for y in xrange(2017,2018):
	for m in xrange(1,7):
		# Get the file
		file_name = '/user/%s/rbda/crime/data/taxi_data/yellow/yellow_tripdata_%d-%02d.csv' % (user,y,m)

		print "Cleaning file:\n\t%s" % file_name

		# Parse the csv by ',' and get a Row object
		dat = sc.textFile(file_name).\
		map(lambda l: l.split(",")).\
		filter(filter_rows).\
		map( lambda l: to_row(l)  )
		
		# Create a DF with the schema.
		df = sqlContext.createDataFrame(dat,schema)

		# Output to save as partition table
		output_folder = '/user/%s/rbda/crime/data/taxi_data_clean/yellow/year=%d/month=%02d' %(user,y,m)
		print 'Saving to hdfs://%s' % output_folder
		df.write.mode('ignore').save(output_folder)
