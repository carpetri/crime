#!/usr/bin/env bash

# Downloads hourly weather data for a given station
# Find the codes and available dates at ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.csv

# Inputs:
#
#  1) Directory name
#  2) US Air Force weather-station code
#  3) WBAN weather-station code
#  4) Set Postgres environment variables or .pgpass

# Output:


# "725030","14732","LA GUARDIA AIRPORT","US","NY","KLGA","+40.779","-073.880","+0003.4","19730101","20171021"

# "725033","94728","NYC CENTRAL PARK","US","NY","KNYC","+40.779","-073.969","+0047.5","19431201","20100731"

# "725033","99999","NEW YORK CITY  CENTRAL PARK","US","NY","","+40.767","-073.983","+0027.0","20100801","20100817"

# "744860","94789","JOHN F KENNEDY INTERNATIONAL AIRPORT","US","NY","KJFK","+40.639","-073.762","+0003.4","19730101","20171021"


# Check for their existence
if [ $# -lt 3 ]
then
  echo "Three arguments required"
  exit 1
fi

#  1. Directory Name where you'll store the data
DIRNAME=$1
#  2. USAF code
USAF=$2
#  3. WBAN code 
WBAN=$3

OUTPUT=$4
 
# NYC
# ./weather.sh data 725033 94728 
# ./weather.sh data 725030 14732 # LA guardia 
# ./weather.sh data 725033 94728 #Central park to 2010
# ./weather.sh data 725033 99999 #Central park 
# ./weather.sh data 744860 94789 #JFK

# "725053","94728","CENTRAL PARK","US","NY","KNYC","+40.779","-073.969","+0042.7","20120801","20171021"
# ./weather.sh data 725053 94728 # CP good one
##  WEATHER-STATION DATA  ##
############################

# download weather-station data
(cd $DIRNAME && wget -N 'ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.csv')

# create schema and table for weather-station data
# psql postgres -v ON_ERROR_STOP=1 -f weather_stations.sql 

# copy data into weather-station data
cat ${DIRNAME}/isd-history.csv | tr [:upper:] [:lower:] | tr ' ' '_' | sed 's/""//g' |
csvsql --query "select usaf, wban, station_name, ctry as country, state, icao, lat, lon, \"elev(m)\" as elevation_meters, \
    date(substr(begin,1,4) || '-' || substr(begin,5,2) || '-' || substr(begin,7,2)) as begin_date, \
    date(substr(end,1,4) || '-' || substr(end,5,2) || '-' || substr(end,7,2)) as end_date from stdin;" > weather_stations.csv
# psql postgres -v ON_ERROR_STOP=1 -c "\copy weather.weather_stations from stdin with csv header;"


# grab beginning and ending years for the given station
begin=$(cat ${DIRNAME}/isd-history.csv | grep -E "${USAF}.*${WBAN}" | cut -d, -f10 | cut -c2-5)
end=$(cat ${DIRNAME}/isd-history.csv | grep -E "${USAF}.*${WBAN}" | cut -d, -f11 | cut -c2-5)

############################
##      WEATHER DATA      ##
############################

# download the zipped files
parallel -j200% '(wget -N -P {1} "ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-lite/{4}/{2}-{3}-{4}.gz")' ::: $DIRNAME ::: $USAF ::: $WBAN ::: $(seq $begin $end)

# unzip
gunzip -c ${DIRNAME}/*.gz | sed 's/-9999/     /g' | in2csv -H -s weather_schema.csv | sed 's/,-,/,,/g' |
csvsql --query "select ${USAF} as usaf, ${WBAN} as wban, year, month, day, hour, air_temp/10.0 as air_temp_celsius, \
    dew_point_temp/10.0 as dew_point_temp_celsius, sea_level_pressure, wind_direction, .36*wind_speed_rate as wind_speed_km_hr, \
    sky_condition_total_coverage_code, liquid_precipitation_depth_dimension_one_hour as liquid_precipitation_mm_one_hour, \
    liquid_precipitation_depth_dimension_six_hours as liquid_precipitation_mm_six_hours from stdin;" > ${DIRNAME}/${OUTPUT}

# cat ${DIRNAME}/${OUTPUT} | psql postgres -v ON_ERROR_STOP=1 -c "\copy weather.weather from stdin with csv header;"

