# CSCI-4380 Final Project

## Contributors

Zack Allen  
Joey Cengel  
Darrian Gardea  
Giri Srinivasan

## Datasets Used

[New York City weather station data](https://www.ncdc.noaa.gov/cdo-web/datasets/GHCND/stations/GHCND:USW00094728/detail) from NOAA  
[Motor vehicle collisions data](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95) from NYC OpenData

## Running Instructions

**Running this software requires python 3.8+**

To initialize the database, run the following in your command line:

```bash
psql -U postgres postgres < db-setup.sql
```

After creating the database, navigate to the topmost project directory and run `python retrieve_data.py` to load the datasets from the internet.

Once the datasets are loaded, run `python load_data.py` to populate the database.  
**Note: This step could take approximately 30 minutes.**

After the database is populated, start the application by running `python application.py`.