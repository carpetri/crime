

hdfs dfs -mkdir /user/cpa253/rbda/crime/data/taxi_data_clean_weather/

hdfs dfs -mkdir /user/cpa253/rbda/crime/data/taxi_data_clean_weather/yellow


spark-submit \
	--conf spark.num.executors=100 \
	--conf spark.driver.memory=32g \
	--conf spark.executor.memory=32g \
	filter_clean_taxi.py & 