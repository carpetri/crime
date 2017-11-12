#!/bin/bash

# rm -rf /Volumes/server-hd/taxi_data
# mkdir /Volumes/server-hd/taxi_data
# cd /Volumes/server-hd/taxi_data

# cat taxi_links | parallel -j 8  wget --verbose

if [ -d ../../data/taxi_data ]
	then
		echo 'Cleaning data folder'
		rm -r ../../data/taxi_data
fi

for f in `cat taxi_links`
do 
	echo 'Getting: ' $f
	wget --verbose $f 
done

mkdir ../../data/taxi_data

mkdir ../../data/taxi_data/fhv
mkdir ../../data/taxi_data/yellow
mkdir ../../data/taxi_data/green

mv  fhv*.csv ../../data/taxi_data/fhv
mv  yellow*.csv ../../data/taxi_data/yellow
mv  green*.csv ../../data/taxi_data/green

mkdir ../../data/taxi_headers/
mkdir ../../data/taxi_headers/fhv
mkdir ../../data/taxi_headers/yellow
mkdir ../../data/taxi_headers/green

for t in yellow green fhv
do
	FOLDER=../../data/taxi_data/$t
	cd $FOLDER
	for FILE in `ls *.csv`
	do 
		echo 'Cleaning: ' $FILE
		head -1 "$FILE" > ../../taxi_headers/$t/$FILE
		tail -n +3 "$FILE" > "$FILE.tmp" && mv "$FILE.tmp" "$FILE"
	done
	cd ../../../src/get_taxi_data/
done


#taxi_zones
wget https://s3.amazonaws.com/nyc-tlc/misc/taxi_zones.zip 
mkdir ../../data/taxi_zones
unzip -d ../../data/taxi_zones taxi_zones.zip
rm *.zip

