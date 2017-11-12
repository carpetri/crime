#!/bin/bash
if [ -d ../data/taxi_data_clean ]
	then
		echo 'Cleaning data folder'
		rm -r ../data/taxi_data_clean
fi

CFOLDER=../data/taxi_data_clean
mkdir $CFOLDER
mkdir $CFOLDER/yellow