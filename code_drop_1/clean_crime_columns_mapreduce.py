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
sqlContext = SQLContext(sc)

schema = StructType([
	StructField("complaint_id",StringType(), True),
	StructField("stating_datetime",TimestampType(), True),
	StructField("ending_datetime",DoubleType(), True),
	StructField("reported_date",DoubleType(), True),
	StructField("ofense_code",StringType(), True),
	StructField("ofense_description",StringType(), True),
	StructField("pd_code",StringType(), True),
	StructField("pd_description",StringType(), True),
	StructField("completed_code",StringType(), True),
	StructField("offense_level",StringType(), True),
	StructField("jurisdiction",StringType(), True),
	StructField("borough",StringType(), True),
	StructField("precinct",StringType(), True),
	StructField("location_description",StringType(), True),
	StructField("premises_description",StringType(), True),
	StructField("park_name",StringType(), True),
	StructField("nycha",StringType(), True),
	StructField("latitude",StringType(), True),	
	StructField("longitude",StringType(), True),
	# StructField("taxi_zone_id",StringType(), True),
])

#Get USER
user = os. environ['USER']
if user not in ['cpa253','vaa238','vm1370']:
	user = 'cpa253'

crimes_folder= "/user/%s/rbda/crime/data/crime" % user

def parse_csv(line):
    """ Function to parse csv"""
    f = cStringIO.StringIO(line)
    readCSV = csv.reader(f, delimiter=',')
    return [row for row in readCSV]

# zones = pd.read_csv('../data/taxi_zones/taxi_zones_clean.csv', sep =' ', names=['lon','lat','LocationID'])

# def get_zone_id(lon,lat, pd_data):
# 	if not lon:
# 		return None
# 	if not lat:
# 		return None
# 	pd_data['dist'] =  (pd_data.lon - float(lon))**2 + (pd_data.lat - float(lat))**2
# 	ind=pd_data.idxmin(axis=0)['dist']
# 	out = pd_data.LocationID[ind]
# 	return str(out)
# zones_broad=sc.broadcast(zones)

def clean_dates(d,t):
	if d=='':
		return None

	if t=='':
		return None	

	if t[0:2] == '24':
		t='00'+t[2:]
	
	if int(d[6:8] ) <= 10 and int(d[8:10]) < 18:
		d = d[0:6] + '20' + d[8:10]

	return datetime.strptime("%s %s" % (d,t), '%m/%d/%Y %H:%M:%S')
		

def clean_empty(x):
	if x == '':
		return None
	else:
		return x

def parse_line(l):
	r = Row(
		"complaint_id",
		"stating_datetime",
		"ending_datetime",
		"reported_date",
		"ofense_code",
		"ofense_description",
		"pd_code",
		"pd_description",
		"completed_code",
		"offense_level",
		"jurisdiction",
		"borough",
		"precinct",
		"location_description",
		"premises_description",
		"park_name",
		"nycha",
		"latitude",
		"longitude",
		# "taxi_zone_id",
		)

	return r(
	  l[0], # complaint_id
	  clean_dates(l[1],l[2]), #stating date
	  clean_dates(l[3],l[4]), #end date
	  datetime.strptime(l[5], '%m/%d/%Y'), #reported date
	  clean_empty(l[6]), #ofense_code
	  clean_empty(l[7]), #ofense_description
	  clean_empty(l[8]), #pd_code
	  clean_empty(l[9]), #pd_description
	  clean_empty(l[10]), #completed_code
	  clean_empty(l[11]), #offense_level
	  clean_empty(l[12]), #jurisdiction
	  clean_empty(l[13]), #borough
	  clean_empty(l[14]), #precinct
	  clean_empty(l[15]), #location_description
	  clean_empty(l[16]), #premises_description
	  clean_empty(l[17]), #park
	  clean_empty(l[18]), #park
	  clean_empty(l[21]), # latitude
	  clean_empty(l[22]), #longitude
	  # get_zone_id(l[21],l[22],zones_broad.value), #taxi_zone
	  # None
	  )


def filter_dates(x):
	if x == '':
		return False

	y=int(x[6:8])
	if y != 19 and y != 20 and int(x[8:10]) > 17:
		return False
	else:
		return True

zones_file = '/user/%s/rbda/crime/data/taxi_zones/taxi_zones_clean.csv' % (user)

zones_rdd = sc.textFile(zones_file).\
	map(lambda x: x.split(' ')).\
	map(lambda (lon,lat,LocationID): 
	    Row(lon=float(lon),
	        lat=float(lat),
	        taxi_zones_id=LocationID)).toDF()


dat = sc.textFile(crimes_folder).\
flatMap(parse_csv).\
filter(lambda l: filter_dates(l[1]) and 
       filter_dates(l[3]) and filter_dates(l[5]) ).\
map(parse_line).\
toDF(schema)

crimes = zones_rdd.join(dat.select(['complaint_id','longitude','latitude']))

crimes_d = crimes.withColumn('dist', (crimes.longitude -crimes.lon)**2 + (crimes.latitude -crimes.lat)**2 ).select('complaint_id','dist','taxi_zones_id')

# crimes_d_fil= crimes_d.filter(crimes_d.complaint_id =='491504201')
crimes_with_id=crimes_d.groupBy('complaint_id').min('dist').show()
# 4258132 * 15000 = 63,871,980,000 ish


output_folder = '/user/%s/rbda/crime/data/crime_data_clean' %(user)

print 'Saving to hdfs://%s' % output_folder
dat.write.mode('overwrite').save(output_folder)



