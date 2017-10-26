#!/bin/bash

# rm -rf /Volumes/server-hd/taxi_data
# mkdir /Volumes/server-hd/taxi_data
# cd /Volumes/server-hd/taxi_data

cat taxi_links | parallel -j 8  wget --verbose
