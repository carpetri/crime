import 

user = os. environ['USER']
if user not in ['cpa253','vaa238','vm1370']:
	user = 'cpa253'

crimes_folder= "/user/%s/rbda/crime/data/crime" % user



dat= sqlContext.read.format('com.databricks.spark.csv').load(crimes_folder)

