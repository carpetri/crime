#!/bin/bash

hdfs dfs -rm -r /user/$USER/rbda/crime/data/crime_clean
#hdfs dfs -mkdir /user/$USER/rbda/crime/data/crime_clean



# spark-submit \
# 	--conf spark.num.executors=100 \
# 	--conf spark.driver.memory=32g \
# 	--conf spark.executor.memory=32g \
# 	clean_crime_columns.py 


spark-submit \
	--conf spark.num.executors=100 \
	--conf spark.driver.memory=32g \
	--conf spark.executor.memory=32g \
	clean_crime_columns.py & 