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

print sc