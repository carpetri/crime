# Predicting crime using taxi trips data - CODE DROP 1

Authors:

* Carlos Petricioli (cpa253@nyu.edu)
* Valerie Angulo (vaa238@nyu.edu)
* Varsha Muralidharan (vm1370@nyu.edu)

`
code_drop_1
|-- [ 492]  README.md
|-- [4.1K]  clean_crime_columns.py
|-- [4.7K]  clean_crime_columns_mapreduce.py
|-- [ 12K]  clean_taxi_columns.py
|-- [ 12K]  clean_taxi_columns_mapreduce.py
|-- [ 878]  transform_taxi_zones.sh
`

This folder includes code that runs mainly on spark. We decided to merge an important part of the analytic into the cleaning phase since it made more sense to process the datasets from scratch just once and create a dataset ready to answer the questions we will need to answer to try to support our hypothesis.

An important task in our analytic requires for us to have the three datasets spatially linked. Our zones are defined in a shape file that defines taxi zones among the 5 boroughs in New York. 

### transform_taxi_zones.sh

To code this in a map reduce sense we decided to transform the taxi zones  shapefile into a `Raster` file which can be thought as a  big dense matrix that contains the (longitude,latitude) in each index and has the *zone_id* as value. The file `transform_taxi_zones.sh` transforms the shapefile into a csv that has the format (longitude,latitude,LocationID) and puts it into HDFS.

## Map + Join + Reduce 

The code in clean_crime_mapreduce.py clean_taxi_columns_mapreduce.py essentially cleans all the columns and assign a Taxi Zone ID to each crime and each taxi ride so that we are able to link them into the analytic. To do this task a join is performed so that each record  gets **mapped** in the taxis and crimes get all the possible values in the taxi_zones_ids.csv and gets **reduced** by computing the minimum distance to assign the zone id. 

We have about 1.2 billion rows in the taxis  and 4.5 million rows in crimes. The zone ids csv has about 15,000 records so we have a about 
1,600,000,000 * 15,000 = 24,000,000,000,000 = 24,000 billion sized table.


## Map + Reduce 

The code in clean_crime_columns.py clean_taxi_columns.py essentially does the same but instead of joining the tables we do the reduce phase after each map task. This is more efficient because we do not have to compute the giant table to be able to reduce.


## OUTPUT

This code produce partition tables as parquet which can be read by Hive or SparkSQL.

The following code can be used to access them from SparkSQL.

```
df = sqlContext.read.option("mergeSchema", "true").parquet(file)
```


The following code can be used to access them from Hive.

```
create external table parquet_table_name (x INT, y STRING)
  ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
  STORED AS 
    INPUTFORMAT "parquet.hive.DeprecatedParquetInputFormat"
    OUTPUTFORMAT "parquet.hive.DeprecatedParquetOutputFormat"
    LOCATION '/test-warehouse/tinytable';
```












