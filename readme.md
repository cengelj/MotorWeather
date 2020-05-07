# CSCI-4380 Final Project

## Contributors

Zack Allen  
Joey Cengel  
Darrian Gardea  
Giri Srinivasan

## Datasets Used

[New York City weather station data](https://www.ncdc.noaa.gov/cdo-web/datasets/GHCND/stations/GHCND:USW00094728/detail) from NOAA  
[Motor vehicle collisions data](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95) from NYC OpenData

## Installation Instructions

_Running this software requires [python 3.8+](https://www.python.org/downloads/release/python-382/) and [Postgres 12](https://www.postgresql.org/download/)_

We recommend the use of a python virtual environment. Documentation for that can be found [here](https://docs.python.org/3/library/venv.html)

After creating your virtual environment, install the dependencies by running
```bash
pip install -r requirements.txt
```

To initialize the database, run the following in your command line:

```bash
psql -U postgres postgres < db-setup.sql
```

After creating the database, navigate to the topmost project directory and run `python retrieve_data.py` to load the datasets from the internet.

Once the datasets are loaded, run `python load_data.py` to populate the database.  
_**Note:** This step could take approximately 30 minutes._

After the database is populated, start the application by running `python application.py`.