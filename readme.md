# CSCI-4380 Final Project Spring 2020

## Contributors

Zack Allen  
Joey Cengel  
Darrian Gardea  
Giri Srinivasan

## Datasets Used

- [NYC Central Park weather station data](https://www.ncdc.noaa.gov/cdo-web/datasets/GHCND/stations/GHCND:USW00094728/detail) from NOAA. We are hosting it [here](https://docs.google.com/spreadsheets/d/11eMgzRgRE0GZLjj5rk0IDqu8fHr3uwVAySXCubIZ5nA/edit?usp=sharing) on Google Drive.  
- [Motor vehicle collisions data](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95) from NYC OpenData

## Installation Instructions

_Running this software requires [python 3.8+](https://www.python.org/downloads/release/python-382/) and [Postgres 12](https://www.postgresql.org/download/)_

We recommend the use of a python virtual environment. Documentation for that can be found [here](https://docs.python.org/3/library/venv.html) .

After creating your virtual environment, install the dependencies by running
```bash
pip install -r requirements.txt
```

To initialize the database, run the following from the parent directory:

```bash
psql -U postgres postgres < db-setup.sql
```

After creating the database, run `python retrieve_data.py` to load the datasets from the internet.

Once the datasets are loaded, enter the directory called `code` and run `python load_data.py` to populate the database.  
_**Note:** This step could take approximately 30 minutes._

After the database is populated, start the application by running `python application.py`.

## Available Queries

**- Crashes by Date:** Displays the number of car crashes on the user-selected date.  
**- Weather by Date:** Displays weather details from the user-selected date.  
**- Most Common Weather:** Displays a ranked list (descending) of the most common weather conditions.  
**- Most Crashed-In Weather:** Displays a ranked list (descending) of the most crashed-in weather conditions.  
**- Deadliest Weather:** Displays a ranked list (descending) of the deadliest weather conditions.  
**- Most Injurious Weather:** Displays a ranked list (descending) of the most injurious weather conditions.  
**- Crashes by Borough:** Displays a ranked list (descending) of NYC boroughs by crash frequency.

## Project Video

A video of the application in use can be found [here](https://drive.google.com/open?id=16hF0sEipgBjYm-AbRPB1d_qn-TbbSuci) .

### Note to Instructor

The NOAA weather data is hosted on Google Drive due to the data being a small portion of a large publicly avaliable dataset which must be downloaded individually using a custom "order".
The dataset is accessible under the Creative Commons license.  

“Open Data is free public data published by New York City agencies and other partners.”