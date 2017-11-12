
ogr2ogr ../data/taxi_zones/taxi_zones_clean.shp ../data/taxi_zones/taxi_zones.shp  -t_srs "+proj=longlat +ellps=WGS84 +no_defs +towgs84=0,0,0"

gdal_rasterize -a LocationID -ts 8000 8000 -l taxi_zones_clean ../data/taxi_zones/taxi_zones_clean.shp ../data/taxi_zones/taxi_zones_raster.tif

# raster2xyz ../data/taxi_zones/taxi_zones_raster.tif ../data/taxi_zones/taxi_zones.csv

gdal_translate -of XYZ ../data/taxi_zones/taxi_zones_raster.tif ../data/taxi_zones/taxi_zones_raster.csv

awk -F ' ' '$3 != 0 { print }' ../data/taxi_zones/taxi_zones_raster.csv > ../data/taxi_zones/taxi_zones_clean.csv