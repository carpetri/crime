#!/bin/bash
if [ -d ../data/taxi_data_clean ]
	then
		echo 'Cleaning data folder'
		rm -r ../data/taxi_data_clean
fi

CFOLDER=../data/taxi_data_clean
mkdir $CFOLDER
mkdir $CFOLDER/yellow

module load gdal/2.2.0
module load xz/5.2.2
module load pygdal/2.2.0.3
module load zlib/1.2.8