#!/usr/bin/env bash

if [ -d ../../data/weather ] 
	then
		rm -rf ../../data/weather
fi

mkdir ../../data/weather

./weather.sh ../../data/weather 725030 14732 laguardia.csv #LA guardia 

./weather.sh ../../data/weather 725033 94728 centralpark_1.csv #Central park to 2010

./weather.sh ../../data/weather 725033 99999 centralpark_2.csv #Central park 

./weather.sh ../../data/weather 744860 94789 jfk.csv #JFK


./weather.sh ../../data/weather 725053 94728 centralpark.csv # CP good one

mv ../../data/weather/isd-history.csv ../../data_dictionaries

rm -rf ../../data/weather/*.gz