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

user = 'cpa253'

y = 2014
m= 1

file_name = '/user/cpa253/rbda/crime/data/taxi_data_clean_weather/yellow/year=%d' %(y)

df = sqlContext.read.option("mergeSchema", "true").parquet(file_name)

