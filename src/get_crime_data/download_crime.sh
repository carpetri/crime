#/bin/bash
if [ ! -d ../../data/crime ]; then
	mkdir ../../data/crime
fi

#https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i
wget --verbose https://data.cityofnewyork.us/api/views/qgea-i56i/rows.csv?accessType=DOWNLOAD -O ../../data/crime/nypd_complaint_hist.csv

wget https://data.cityofnewyork.us/api/views/qgea-i56i/files/82bbfb3b-e81c-4371-ba33-1dc7819ab447?download=true&filename=NYPD_Incident_Level_Data_Column_Descriptions.csv -O ../../data/crime/nypd_incident_level_data_column_descriptions.csv


wget --verbose https://data.cityofnewyork.us/resource/9s4h-37hy.csv -O ../../data/crime/nypd_complaint_hist.csv

# CURRENT 
wget --verbose https://data.cityofnewyork.us/api/views/5uac-w243/rows.csv?accessType=DOWNLOAD -O ../../data/crime/nypd_complaint_current.csv
