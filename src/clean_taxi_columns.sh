#!/bin/bash
# if [ -d ../data/taxi_data_clean ]
# 	then
# 		echo 'Cleaning data folder'
# 		rm -r ../data/taxi_data_clean
# fi

# CFOLDER=../data/taxi_data_clean
# mkdir $CFOLDER
# mkdir $CFOLDER/yellow

#Make sure that you have the taxi_zones


## This cleans HDFS and prepares it for spark's partition table
hdfs dfs -rm -r /user/$USER/rbda/crime/data/taxi_data_clean/yellow

hdfs dfs -mkdir /user/$USER/rbda/crime/data/taxi_data_clean/yellow



module load gdal/2.2.0
module load xz/5.2.2
module load pygdal/2.2.0.3
module load zlib/1.2.8

spark-submit \
	--conf spark.num.executors=100 \
	--conf spark.driver.memory=64g \
	--conf spark.executor.memory=32g \
	clean_taxi_columns.py &


