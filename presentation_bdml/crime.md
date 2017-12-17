# `r params$set_title`
12/17/2017  

### Project Title

 Predicting crime rates using taxi rides in NYC


### Abstract

- This study looks at the relationship between crime rates and taxi usage in New York City. 
- My hypothesis is that people are less likely to walk in areas subjectively deemed more dangerous and will instead opt to use more reliable and immediate transportation such as designated taxis. 
- There is  evidence that supports this hypothesis. But by time I still do not have a model.

## Motivation

### Importance 
- This project can help law enforcement predict areas of crime based on New Yorkers transportation habits. Law enforcement officials may be able to predict which areas will have a higher rate of crime in the future.
- People who live in an area are aware of the safety of their surroundings and this awareness can be represented by how comfortable residents may be in walking or taking the subway versus taking more immediate, more expensive, modes of transportation such as taxis. 
- This project can benefit the community and tourists by influencing their current and future transportation behaviors 


## Data Sources


### Taxi rides data from TLC [(*Link*)](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)


- It covers years from 2009 to June 2017.

- The yellow taxi trip records include:

  + pick-up and drop-off dates/times, 
  + pick-up and drop-off locations, 
  + trip distance,
  + itemized fares, 
  + rate types, 
  + payment type, 
  + passenger counts. 

- *Data Size*: 250 GB

***

### NYPD Complaint Data  [(*Link 1,*](https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Historic/qgea-i56i) [*Link 2)*](https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Current-YTD/5uac-w243)

- This dataset includes all valid felony, misdemeanor, and violation crimes reported to the New York City Police Department (NYPD) from 2006 to year to date data.

- *Data Size*:  1.5 GB

### NOAA Weather stations data [*(Link)*](https://www.ncdc.noaa.gov/isd)

The *Integrated Surface Database (ISD)* consists of global hourly ansynoptic observations compiled from numerous sources into a single common ASCII format and common data model.

- ISD's complete history of hour-by-hour readings for one user-specified weather stations

- I selected:
  + Central Park
  + JFK 
  + Laguardia

- *Data Size*:  165 MB

## Obstacles

### Cleaning the data: Taxis

- The **taxi data** (250 GB) was the most challenging to clean.

- Inconsistencies in columns: extra columns for some years.

- Rows with extra commas: avoiding an easy parse.

- Row values for each year were not that dirty but the data values were completely different for different years.

- The dictionary that defines the labels refers to the data from 2017, so I needed to figure out the meaning of labels for previous years.


***

### Cleaning the data: Taxis

- To figure this out I needed to iterate through every row of the data because new things came up every time I thought I was done with the cleaning.
  - Even after I cleaned all the categorical variables and I thought I was done, many numerical inconsistencies appeared.
  - Longitude/Latitude, like not even in NY or simply null.
  - Negative, but consistent values for amounts.
  - Weird trip distances, like greater than 1000 miles.
  - Exorbitant total amounts (which might not be a problem because most were negotiated fares)

***

### Cleaning the data: Taxis
  - 2016 and 2017 do not have longitude and latitude, just zone id. 
  - This became one of the greater obstacles, because I needed to assign a zone id to all the previous years. 
  - About 1.2 billion x 14 thousand $\approx$ 16,800 $\approx$ 2 1.6x10^ 13 distances computed (just for pick-up) 
  \begin{figure}
  \label{fig:zones}
    \centering
    \begin{subfigure}[t]{0.45\textwidth}
        \centering
        \label{fig:zones_shape}
        \includegraphics[height=1.4in]{../latex/images/taxi_zones_shape}
        \caption{Shape}
    \end{subfigure}%
    ~ 
    \begin{subfigure}[t]{0.45\textwidth}
        \centering
        \label{fig:zones_raster}
        \includegraphics[height=1.4in]{../latex/images/taxi_zones_raster}
        \caption{Raster}
    \end{subfigure}
    \caption{NYC Taxi zones file formats}
  \end{figure}

***

\begin{figure}
    \begin{subfigure}[t]{0.8\textwidth}
        \centering
        \label{fig:zones_both}
        \includegraphics[width=0.5\textwidth]{../latex/images/both}
        \caption{Both}
    \end{subfigure}
    \caption{NYC Taxi zones file formats}
\end{figure}

***

### Cleaning the data: Crime

- Dates needed to be cleaned. (**24:00:00 vs 00:00:00**)

- Meaningful interpretations of other dates could not be made for certain records and these records had to be filtered for example:
  - 1016 $\rightarrow$ 2016. 
  - 1026 $\rightarrow$ dropped.

- The most challenging factor here was that I had a lot of missing values for some columns so I needed to setup a schema that accepted this fact.

### Cleaning the data: Weather

- Weather data was the most decent. 
- I basically just checked that the data was clean.
- The only major issue was to figure out a way to assign weather data to the taxis.


***

### Joining the data

- In the cleaning process I assigned taxi zones for taxi pickup and drop-off locations.

- So I repeated this process but this time instead of assigning zones I assigned a station (JFK, La Guardia, Central Park) by computing the min distance from the pickup locations (long/lat) to the weather station.
- I had another problem here because I did not have (long/lat) for the recent data. 
- So, I estimated the centroids on the pick-up zones and then computed the min distance to the weather stations.

- Finally taxis were joined to crime by using time periods of one hour. 
 

## Goodness

### Consistent data

- One of the main concerns was the consistency of the data through time and among the different sources, so I made a lot of effort to keep all variables, even the ones I ended up not using.

### Empirical observations not causality
- Up to now  I am  not trying to explain causality so the observations should be interpreted as empirical correlations and raw insight obtained from a very  long cleaning data phase

## Results
- Opposite colors support the hypothesis.
\begin{figure}
  \label{fig:zones}
    \centering
    \begin{subfigure}[t]{0.45\textwidth}
        \centering
        \label{fig:zones_shape}
        \includegraphics[width=1\textwidth]{../img/crimes_per_zone_2015_Manhattan}
        \caption{Crimes in Manhattan}
    \end{subfigure}%
    ~ 
    \begin{subfigure}[t]{0.45\textwidth}
        \centering
        \label{fig:zones_raster}
        \includegraphics[width=1\textwidth]{../img/taxis_2015_Manhattan}
        \caption{Pickups in Manhattan}
    \end{subfigure}
    \caption{Crime and Pickups in Manhattan, 2015}
  \end{figure}

***
\begin{figure}
  \label{fig:zones}
    \centering
    \begin{subfigure}[t]{0.45\textwidth}
        \centering
        \label{fig:zones_shape}
        \includegraphics[width=1\textwidth]{../img/crimes_per_zone_2015_Queens}
        \caption{Crimes in Queens}
    \end{subfigure}%
    ~ 
    \begin{subfigure}[t]{0.45\textwidth}
        \centering
        \label{fig:zones_raster}
        \includegraphics[width=1\textwidth]{../img/taxis_2015_Queens}
        \caption{Pickups in Queens}
    \end{subfigure}
    \caption{Crime and Pickups in Queens, 2015}
  \end{figure}

***

### Crimes and taxis

\begin{figure} 
\centering
\includegraphics[width=1\textwidth]{../img/scatter_crimes_taxis_pres.pdf}
\caption{Crimes and pickups per zone}
\end{figure}

***

### Results when considering Rain

\begin{figure} 
\centering
\includegraphics[width=1\textwidth]{../img/scatter_crimes_taxis_rain.pdf}
\caption{Crimes and hourly pickups per zone in rain}
\end{figure}


## Summary

- I collected NYC taxi trip data, NYC crime data and weather data from Central Park, JFK and LaGuardia and I was able to join everything at a very granular level.

- I found evidence that suggests that the hypothesis might be true, places that have higher levels of crime showed evidence of having a higher number of pickups, especially when taking rain into account.


## References

\begin{thebibliography}{1}

\bibitem{Bendler14}
J.~Bendler, T.~Brandt, S.~Wagner, and D.~Neumann.
\newblock Investigating crime-to-twitter relationships in urban environments -
  facilitating a virtual neighborhood watch.
\newblock In M.~Avital, J.~M. Leimeister, and U.~Schultze, editors, {\em ECIS},
  2014.

\bibitem{Wang16}
H.~Wang, D.~Kifer, C.~Graif, and Z.~Li.
\newblock Crime rate inference with big data.
\newblock In {\em Proceedings of the 22Nd ACM SIGKDD International Conference
  on Knowledge Discovery and Data Mining}, KDD '16, pages 635--644, New York,
  NY, USA, 2016. ACM.

\bibitem{Traunmueller14}
M.~Traunmueller, G.~Quattrone, and L.~Capra.
\newblock {\em Mining Mobile Phone Data to Investigate Urban Crime Theories at
  Scale}, pages 396--411.
\newblock Springer International Publishing, Cham, 2014.
\end{thebibliography}

*** 

\begin{thebibliography}{1}
\bibitem{OUAC}
A.~Bogomolov, B.~Lepri, J.~Staiano, N.~Oliver, F.~Pianesi, and A.~Pentland.
\newblock Once upon a crime: Towards crime prediction from demographics and
  mobile data, Sep 2014.

\bibitem{Chainey}
S.~Chainey, L.~Tompson, and S.~Uhlig.
\newblock {The Utility of Hotspot Mapping for Predicting Spatial Patterns of
  Crime}.
\newblock {\em Security Journal}, 21(1-2):4--28, Feb 2008.

\bibitem{visCrime}
T.~Nakaya and K.~Yano.
\newblock Visualising crime clusters in a space-time cube: An exploratory
  data-analysis approach using space-time kernel density estimation and scan
  statistics.
\newblock {\em Transactions in GIS}, 14(3):223--239, 2010.
\end{thebibliography}

