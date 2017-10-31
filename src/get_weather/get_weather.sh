#!/usr/bin/env bash

if [ -d ../../data/crime ] 
	then
		rm -rf ../../data/crime
fi

mkdir ../../data/crime

./weather.sh ../../data/crime 725030 14732 laguardia.csv #LA guardia 

./weather.sh ../../data/crime 725033 94728 centralpark_1.csv #Central park to 2010

./weather.sh ../../data/crime 725033 99999 centralpark_2.csv #Central park 

./weather.sh ../../data/crime 744860 94789 jfk.csv #JFK


./weather.sh ../../data/crime 725053 94728 centralpark.csv # CP good one

rm -rf ../../data/crime/*.gz