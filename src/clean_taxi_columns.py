from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.functions import *
from datetime import datetime, tzinfo
import os
from osgeo import *
from osgeo import ogr
import sys

# read your shapefile
drv    = ogr.GetDriverByName('ESRI Shapefile')
ds_in  = drv.Open("../data/taxi_zones/taxi_zones_clean.shp")
lyr_in = ds_in.GetLayer(0)
geo_ref = lyr_in.GetSpatialRef()
idx_reg = lyr_in.GetLayerDefn().GetFieldIndex("LocationID")

def check_zone(lon, lat):
	# [lon,lat,z]=ctran.TransformPoint(lon,lat)
	pt = ogr.Geometry(ogr.wkbPoint)
	pt.SetPoint_2D(0, lon, lat)
	lyr_in.SetSpatialFilter(pt)
	for feat_in in lyr_in:
	    loc = feat_in.GetFieldAsString(idx_reg)
	    if loc:
	    	return loc


# FOR DATA before 2015

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

# dat09_14 = sqlContext.createDataFrame(schema)
user = os. environ['USER']
# if user not in ['cpa253','vaa238','vm1370']:
# 	user = 'cpa253'

#NOT FOR 2009
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
		return -1
	else:
		return out

def clean_rate_code(x):
	if x not in xrange(1,7):
		return 'Empty'
	else:
		rate_code_dict= {
			"1":"Standard rate",
			"2":"JFK",
			"3":"Newark",
			"4":"Nassau or Westchester",
			"5":"Negotiated fare",
			"6":"Group ride"
			}
		return rate_code_dict[x]

def clean_store_flag(x):
	flag_dict= {
	' ':'Empty',
	'':'Empty',
	'*':'Empty',
	'2':'Empty',
	"1":"Stored",
	"Y":"Stored",
	"0":"Not a stored",
	"N":"Not a stored",
	}
	return flag_dict[x]

#ONLYU AFTER 2009
def clean_payment_type(x):

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
	if x != '':
		return float(x)
	else:
		return float(0)


r =Row( 
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


def clean_imp_sur(x):
	if year < 2015:
		return 0.0
	else:
		return float(x)

def to_row(l):
	if year <= 2014:
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
		    -1, #PICKUP ZONE
			-1, #DROP ZONE
			)
		return out
	
	if year == 2015:
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
		    clean_imp_sur(17), #improvement_surcharge
		    to_double(l[18]), #total_amount
    		-1, #PICKUP ZONE
			-1, #DROP ZONE
			)
		return out

	if year == 2016 and month <= 6:
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
		    clean_imp_sur(17), #improvement_surcharge
		    to_double(l[18]), #total_amount
    		-1, #PICKUP ZONE
			-1, #DROP ZONE
			)
		return out
	else:
		out = r(
			clean_vendor_name(l[0]), #vendor_name
		    to_date(l[1]), #pickup_datetime 
		    to_date(l[2]), #dropoff_datetime 
		    clean_passenger_count(l[3]), #passenger_count 
		    to_double(l[4]),  #trip_distance 
		    # to_double(l[5]),  #pickup_longitude 
		    # to_double(l[6]),  #pickup_latitude
		    clean_rate_code(l[5]),  #rate_code
		    clean_store_flag(l[6]),  #store_and_fwd_flag
		    # to_double(l[9]),  #dropoff_longitude
		    # to_double(l[10]), #dropoff_latitude
		    clean_payment_type(l[9]), #payment_type
		    to_double(l[10]), #fare_amount
		    to_double(l[11]), #extra
		    to_double(l[12]), #mta_tax
		    to_double(l[13]), #tip_amount
		    to_double(l[14]), #tolls_amount
		    clean_imp_sur(15), #improvement_surcharge
		    to_double(l[16]), #total_amount
		    l[7], #PICK ID
			l[8], #DROP ID
			)
		return out

	



# # Cleaning 2009
# for year in [2009]:
# 	for month in xrange(1,13):
# 		file_name = '/user/%s/rbda/crime/data/taxi_data/yellow/yellow_tripdata_%d-%02d.csv' % (user,year,month)

# 		print "Cleaning file:\n\t%s" % file_name

# 		dat = sc.textFile(file_name).map(lambda l: l.split(",")).\
# 		map(lambda l: r(
# 			l[0], #vendor_name
# 		    to_date(l[1]), #pickup_datetime 
# 		    to_date(l[2]), #dropoff_datetime 
# 		    clean_passenger_count(l[3]), #passenger_count 
# 		    to_double(l[4]),  #trip_distance 
# 		    to_double(l[5]),  #pickup_longitude 
# 		    to_double(l[6]),  #pickup_latitude
# 		    clean_rate_code(l[7]),  #rate_code
# 		    clean_store_flag(l[8]),  #store_and_fwd_flag
# 		    to_double(l[9]),  #dropoff_longitude
# 		    to_double(l[10]), #dropoff_latitude
# 		    l[11].lower().capitalize(), #payment_type
# 		    to_double(l[12]), #fare_amount
# 		    to_double(l[13]), #extra
# 		    to_double(l[14]), #mta_tax
# 		    to_double(l[15]), #tip_amount
# 		    to_double(l[16]), #tolls_amount
# 		    clean_imp_sur(0.0), #improvement_surcharge
# 		    to_double(l[17]), #total_amount
# 			)
# 		)
# 		df = sqlContext.createDataFrame(dat,schema)
# 		## OUT
# 		output_folder = '/user/%s/rbda/crime/data/taxi_data_clean/yellow/year=%d/month=%02d' %(user,year,month)
# 		print 'Saving to hdfs://%s' % output_folder
# 		df.write.mode('overwrite').save(output_folder)

#cleaning 2009 - 2015

def filter_rows(l):
	if year < 2015 and len(l)==18:
		return True

	if year == 2015 and len(l)==19:
		return True

	if year == 2016 and month <= 6 and len(l)== 19:
		return True

	if year == 2016 and month > 6 and len(l)== 17:
		return True

	if year == 2017 and len(l)== 17:
		return True

	return False


# for year in xrange(2009,201):
for year in xrange(2009,2016):
	for month in xrange(1,13):
		file_name = '/user/%s/rbda/crime/data/taxi_data/yellow/yellow_tripdata_%d-%02d.csv' % (user,year,month)

		print "Cleaning file:\n\t%s" % file_name

		dat = sc.textFile(file_name).map(lambda l: l.split(",")).filter(filter_rows).map(lambda l: to_row(l))
		df = sqlContext.createDataFrame(dat,schema)
		## OUT
		output_folder = '/user/%s/rbda/crime/data/taxi_data_clean/yellow/year=%d/month=%02d' %(user,year,month)
		print 'Saving to hdfs://%s' % output_folder
		df.write.mode('overwrite').save(output_folder)






# df_2009_folder = '/user/%s/rbda/crime/data/taxi_data_clean/yellow/yellow_tripdata_%d' %(user,2009)

# df_09 = sqlContext.read.parquet(df_2009_folder)


# df_09.select('pickup_datetime')




