#!/bin/bash

# rm -rf /Volumes/server-hd/taxi_data
# mkdir /Volumes/server-hd/taxi_data
# cd /Volumes/server-hd/taxi_data

# cat taxi_links | parallel -j 8  wget --verbose

for f in `cat taxi_links`
do 
	echo 'Getting: ' $f
	wget --verbose $f 
done

mkdir ../../data/taxi_data
mv -r *.csv ../../data/taxi_data
