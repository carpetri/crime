#/bin/bash

USER=$(whoami)
PROJ_HOME_HDFS=/user/$USER/rbda/crime
DATA_HOME_HDFS=$PROJ_HOME_HDFS/data


## CLEAN ALL DATA IF ANY
hdfs dfs -rm -r $DATA_HOME_HDFS

# SETUP FOLDERS
hdfs dfs -mkdir /user/$USER/rbda
hdfs dfs -mkdir $PROJ_HOME_HDFS
hdfs dfs -mkdir $PROJ_HOME_HDFS/data


# COPY ALL DATA FROM LOCAL REPO
# MAKE SURE YOU DONT CHANGE THE NAME OF THE FOLDER
# MAKE SURE YOU save it under /home/netid/

### IT TAKES A WHILE about 250GB of data
hdfs dfs -copyFromLocal -f \
	/home/$USER/crime/data \
	$PROJ_HOME_HDFS


hdfs dfs -ls -R $DATA_HOME_HDFS | awk '{print $8}' | \
sed -e 's/[^-][^\/]*\//--/g' -e 's/^/ /' -e 's/-/|/'
