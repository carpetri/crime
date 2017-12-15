# Predicting crime using taxi trips data

Authors:

* Carlos Petricioli (cpa253@nyu.edu)
* Valerie Angulo (vaa238@nyu.edu)
* Varsha Muralidharan (vm1370@nyu.edu)

<!-- CODE COVERAGE HERE -->
![presentation](/presentation/crime.html)
## Project Description

Understanding and predicting crime is a crucial task in any mayor city. The objective is to understand crime rates at a granular level with the idea that people behave according on how secure they feel and this fact impact the way they travel. It might be that people prefer to take a taxi versus other options depending on their own perception of crime in their current location. This work will analyze taxi and crime data on a case level.
This is a modern approach that will complement the use of demographics and geographical variables commonly used to predict crime. Global Positioning System (GPS) data on taxi rides provide useful information that can be directly related to crime at a block level. There is enough data to make this analysis possible. This work will be limited to the City of New York.

### Typical user of this application

The scientific community, the citizens and demographics themselves.

### Insight that we'll get

The objective is to get a better sense of the way that crime relates to the use of taxis in NYC.

### Testing

We will use traditional machine learning standards such as Train/Test data, cross validation and model validations where applicable.

## Taxi rides data form TLC

[Download here](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)

The yellow and green taxi trip records include fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver- reported passenger counts. It covers years from 2009 to June 2017.

## NYPD Complaint Data Historic and Current YTD Data 

### Historic

[Download here](https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i)

This dataset includes all valid felony, misdemeanor, and violation crimes reported to the New York City Police Department (NYPD) from 2006 to the end of last year (2016).

### Current YTD

[Download here](https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Current-YTD/5uac-w243)

This dataset includes all valid felony, misdemeanor, and violation crimes reported to the New York City Police Department (NYPD) for all complete quarters so far this year (2017).


## NOAA NYC Weather stations data (Central Park, JFK and Laguardia)

[Go here](https://www.ncdc.noaa.gov/isd)

The Integrated Surface Database (ISD) consists of global hourly ansynoptic observations compiled from numerous sources into a single common ASCII format and common data model. ISD's complete history of hour-by-hour readings for one user-specified weather stations


## Software Prerequisites

You will need to have installed:

* [csvkit](https://pypi.python.org/pypi/csvkit)
* [`parallel`](https://www.gnu.org/software/parallel/)

### Get data

Clone this repo and get data for Taxis and Crime. The weather data is already included in the repo.

    git clone https://github.com/RBDA-F17/crime.git
    cd crime/src/get_taxi_data
    chmod +x download_taxi_data.sh
    ./download_taxi_data.sh
    cd ../get_crime_data.sh
    chmod +x download_crime.sh
    ./download_crime.sh
