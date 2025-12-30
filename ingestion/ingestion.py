import logging

from pymongo import MongoClient
import pandas as pd
import requests
import os
from dotenv import load_dotenv
from datetime import datetime

# configure logging
logging.basicConfig(
    filename="ingestion.log",
    level=logging.INFO,
    format="%(levelname)s:%(asctime)s:%(message)s"
)


# Define variables
base_url = 'https://api.weatherbit.io/v2.0/forecast/daily'
params = {
    'lat': '9.896527',  # Jos N source: maps of world
    'lon': '8.858331',  # Jos E
    'key': os.getenv("API_KEY")
        }

# MongoDB config
MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASS = os.getenv("MONGO_PASS")
CONNECTION_STRING = f"mongodb://{MONGO_USER}:{MONGO_PASS}@localhost:27017/"
client = MongoClient(CONNECTION_STRING)
DB = client["nomba"]
COLLECTION = DB["users"]


def api_to_mongo(
        url: str,
        querystrings: dict,
        headers: dict | None = None
        ):
    """
    Function to extract data from an endpoint and store json format data.
    :params url: base url of request
    :params querystrings: url endpoints
    :params s3_key: path/to/filename in s3 bucket
    """
    try:
        response = requests.get(url, params=querystrings, headers=headers)
        if response.status_code == 200:
            response = response.json()
            data = pd.json_normalize(response)
            logging.info("upload complete!")
            return data.head()
        else:
            logging.info("Error!!!", response.text)
    except Exception as e:
        logging.info("Connection error:", e)