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

FOLDER=../../data/taxi_data/fhv
for FILE in `ls $FOLDER *.csv`
do 
	echo 'Cleaning: ' $FILE
	head -1 "$FOLDER/$FILE" > ../../data/taxi_headers/fhv/$FILE
	tail -n +2 "$FOLDER/$FILE" > "$FOLDER/$FILE.tmp" && mv "$FOLDER/$FILE.tmp" "$FOLDER/$FILE"
done

FOLDER=../../data/taxi_data/yellow
for FILE in `ls $FOLDER *.csv`
do 
	echo 'Cleaning: ' $FILE
	head -1 "$FOLDER/$FILE" > ../../data/taxi_headers/yellow/$FILE
	tail -n +3 "$FOLDER/$FILE" > "$FOLDER/$FILE.tmp" && mv "$FOLDER/$FILE.tmp" "$FOLDER/$FILE"
done

FOLDER=../../data/taxi_data/green
for FILE in `ls $FOLDER *.csv`
do 
	echo 'Cleaning: ' $FILE
	head -1 "$FOLDER/$FILE" > ../../data/taxi_headers/green/$FILE
	tail -n +3 "$FOLDER/$FILE" > "$FOLDER/$FILE.tmp" && mv "$FOLDER/$FILE.tmp" "$FOLDER/$FILE"
done