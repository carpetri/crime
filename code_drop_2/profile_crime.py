import os
import sys
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.functions import *
from datetime import datetime
import pandas as pd

user = os. environ['USER']
if user not in ['cpa253','vaa238','vm1370']:
	user = 'cpa253'

file = '/user/%s/rbda/crime/data/crime_clean' %(user)

df = sqlContext.read.parquet(file)

df.printSchema()

# df_pd = df.rdd.map(lambda r: r.asDict())



#pyspark --packages com.databricks:spark-csv_2.10:1.5.0

file_out = '/user/%s/rbda/crime/data/crime_clean_csv' %(user)

df.write.\
	mode('overwrite').\
	format("com.databricks.spark.csv").\
	option("header", "true").\
	option("nullValue",'').\
	save(file_out)




