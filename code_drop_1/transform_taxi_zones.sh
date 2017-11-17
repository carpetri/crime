module load gdal/2.2.0
module load xz/5.2.2
module load pygdal/2.2.0.3
module load zlib/1.2.8

ogr2ogr ../data/taxi_zones/taxi_zones_clean.shp ../data/taxi_zones/taxi_zones.shp  -t_srs "+proj=longlat +ellps=WGS84 +no_defs +towgs84=0,0,0"


gdal_rasterize -a LocationID -ts 200 200 -l taxi_zones_clean ../data/taxi_zones/taxi_zones_clean.shp ../data/taxi_zones/taxi_zones_raster.tif

# raster2xyz ../data/taxi_zones/taxi_zones_raster.tif ../data/taxi_zones/taxi_zones.csv

gdal_translate -of XYZ ../data/taxi_zones/taxi_zones_raster.tif ../data/taxi_zones/taxi_zones_raster.csv

awk -F ' ' '$3 != 0 { print }' ../data/taxi_zones/taxi_zones_raster.csv > ../data/taxi_zones/taxi_zones_clean.csv


# USER=$(whoami)
# PROJ_HOME_HDFS=/user/$USER/rbda/crime
# DATA_HOME_HDFS=$PROJ_HOME_HDFS/data

# hdfs dfs -copyFromLocal -f \
# 	/home/$USER/crime/data/taxi_zones \
# 	$DATA_HOME_HDFS