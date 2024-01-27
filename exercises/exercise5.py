import os
import urllib.request
import zipfile
import re
import pandas as pd
from sqlalchemy.types import Integer, Text, Float


DATA_URL = "https://gtfs.rhoenenergie-bus.de/GTFS.zip"
DATA_FILE_NAME = "stops.txt"
COLUMNS = ["stop_id", "stop_name", "stop_lat", "stop_lon", "zone_id"]
UMLAUTE = ["ä", "ü", "ö"]
STOP_ZONE = 2001
SQL_PATH = "/gtfs.sqlite"
ABS_PATH = os.path.dirname(__file__)


# download zipfile to temp folder using urlretrieve
filehandle, _ = urllib.request.urlretrieve(DATA_URL, os.path.join(ABS_PATH, "GTFS.zip"))

# create a zipefile object
zip_obj = zipfile.ZipFile(filehandle, "r")

# file handle for the file containing our data
stops_file = zip_obj.open(DATA_FILE_NAME)

# read file as csv
stops_df = pd.read_csv(stops_file)

# whitelist columns
stops_df = stops_df[COLUMNS]

# cleaning
# 1) only keep columns where stop_name contains an umlaut
#stops_df = stops_df[stops_df["stop_name"].str.contains("|".join(UMLAUTE), regex=True, flags=re.IGNORECASE)]

# 2) validate lat/long range
stops_df = stops_df[stops_df["stop_lat"] <= 90]
stops_df = stops_df[stops_df["stop_lat"] >= -90]
stops_df = stops_df[stops_df["stop_lon"] <= 90]
stops_df = stops_df[stops_df["stop_lon"] >= -90]

# 3) only keep stops from zone 2001

stops_df = stops_df[stops_df["zone_id"] == STOP_ZONE]




print(stops_df.info())
print(stops_df.head())
print(stops_df.shape)
    
# save data to sqlite db, setting the correct types
stops_df.to_sql("stops","sqlite://" + SQL_PATH, if_exists='replace', dtype={
    "stop_id": Integer,
    "stop_name": Text,
    "stop_lat": Float,
    "stop_lon": Float,
    "zone_id": Integer}, index=False)