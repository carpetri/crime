# Predicting crime using taxi trips data - CODE DROP 2

Authors:

* Carlos Petricioli (cpa253@nyu.edu)
* Valerie Angulo (vaa238@nyu.edu)
* Varsha Muralidharan (vm1370@nyu.edu)

`
code_drop_2
|-- [1.1K]  README.md
|-- [2.9K]  analyze_crime.py
|-- [ 559]  analyze_taxi.py
|-- [1.2K]  filter_clean_crime.py
|-- [3.4K]  filter_clean_taxi.py
|-- [ 677]  profile_crime.py
|-- [1.2K]  profile_taxi.py
`

This folder includes code that runs on spark. This code drop was focused mostly in relating the 3 datasets, crime, weather and taxis. This involved a much more extensive cleaning phase than we expected, so we needed to iterate more in the cleaning. Taxi data was specially complicated. We had to do a lot of assumptions to deal with the 'ugly' data.

This code also includes a first dummy implementation of a linear model that uses Spark's ML-lib.

## OUTPUT

The result of this process leads to all datasets being related.

- Taxi  rides have been joined with  hourly weather data form the closest station to the pickup location.
- Crimes data have now a weather station assigned hourly.









