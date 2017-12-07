from pyspark.sql.functions import col

df1.alias('a').join(df2.alias('b'),col('b.id') = 
	col('a.id')).select([col('a.'+xx) for xx in a.columns] + 
	[col('b.other1'),col('b.other2')])




sc.setLogLevel("WARN")
#setup the same way you have it
#log_txt=sc.textFile("/path/to/data/sample_data.txt")

#crime
file = '/user/%s/rbda/crime/data/crime_clean' %(user)
#or taxi
#file = '/user/%s/rbda/crime/data/taxi_data_clean/yellow' % (user)

log_txt = sqlContext.read.parquet(file)

header = log_txt.first()

#filter out the header, make sure the rest looks correct
log_txt = log_txt.filter(lambda line: line != header)
log_txt.take(10)
  [u'0\\tdog\\t20160906182001\\tgoogle.com', u'1\\tcat\\t20151231120504\\tamazon.com']

temp_var = log_txt.map(lambda k: k.split("\\t"))

#here's where the changes take place
#this creates a dataframe using whatever pyspark feels like using (I think string is the default). the header.split is providing the names of the columns
log_df=temp_var.toDF(header.split("\\t"))
#log_df.show()
#note log_df.schema
#StructType(List(StructField(field1,StringType,true),StructField(field2,StringType,true),StructField(field3,StringType,true),StructField(field4,StringType,true)))

# now lets cast the columns that we actually care about to dtypes we want
log_df = log_df.withColumn("field1Int", log_df["field1"].cast(IntegerType()))
log_df = log_df.withColumn("field3TimeStamp", log_df["field1"].cast(TimestampType()))

#log_df.show()

log_df.schema
StructType(List(StructField(field1,StringType,true),StructField(field2,StringType,true),StructField(field3,StringType,true),StructField(field4,StringType,true),StructField(field1Int,IntegerType,true),StructField(field3TimeStamp,TimestampType,true)))

#now let's filter out the columns we want
#log_df.select(["field1Int","field3TimeStamp","field4"]).show()


# Merge 2 columns with a condition
...
from pyspark.sql.functions import udf

def merge(*c):
    merged = sorted(set(c))
    if len(merged) == 1:
        return merged[0]
    else:
        return "[{0}]".format(",".join(merged))

merge_udf = udf(merge)

df = sqlContext.createDataFrame([("foo", "bar","too","aaa"), 
	("bar", "bar","aaa","foo")], ("k1", "k2" ,"v1" ,"v2"))

df.select(merge_udf("k1", "k2"), merge_udf("v1", "v2"))
...