import os
import sys
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.functions import *
from datetime import datetime

user = os. environ['USER']
if user not in ['cpa253','vaa238','vm1370']:
	user = 'cpa253'

y= 2010
file = '/user/%s/rbda/crime/data/taxi_data_clean/yellow/year=%d' % (user,y)

df = sqlContext.read.option("mergeSchema", "true").parquet(file)

df.printSchema()
df.select('month').distinct().show()
df.rate_code



# dat = [Row(age=1),Row(age=None)]

# schema = StructType([StructField("age",StringType(), True),])
# df = sqlContext.createDataFrame(dat,schema)
# df.show()

for col,t in df.dtypes:
	if t == 'string':
		df.select(col).distinct().show()
	else:
		if t!= 'timestamp':
			df.describe(col).show()


samp = df.sample(False,0.001 )    
s= samp.toPandas()            
s.to_csv('../data/sample_taxis.csv')




