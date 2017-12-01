import os
import sys
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.functions import *
from datetime import datetime

user = os. environ['USER']
if user not in ['cpa253','vaa238','vm1370']:
	user = 'cpa253'

# y= 2016
file = '/user/%s/rbda/crime/data/taxi_data_clean/yellow' % (user)

df = sqlContext.read.option("mergeSchema", "true").parquet(file)

# df.select('month').distinct().show()


# dat = [Row(age=1),Row(age=None)]

# schema = StructType([StructField("age",StringType(), True),])
# df = sqlContext.createDataFrame(dat,schema)
# df.show()

for col,t in df.dtypes:
	if t == 'string':
		d = df.select(col).distinct()
		d.show()
		# print d.toPandas().sort_values(by=col).to_latex(index=False)
	else:
		if t!= 'timestamp':
			d = df.describe(col)
			d.show()
		# print d.toPandas().to_latex(index=False)

# samp = df.sample(False,0.001 )    
# s= samp.toPandas()            
# s.to_csv('../data/sample_taxis.csv')


# a = sc.parallelize([
#                    (Row(age=1,h=2)),
#                    (Row(age=2,h=3))
#                    ]).toDF()     

# b= sc.parallelize([
#                   Row(lon='a',lat='b'),
#                   Row(lon='b',lat='c')]).toDF()













