import requests
from pymongo import MongoClient
import pandas as pd
import os
import logging
from dotenv import load_dotenv
from datetime import datetime
from pymongo.errors import PyMongoError

# configure logging
logging.basicConfig(
    filename="ingestion.log",
    level=logging.INFO,
    format="%(levelname)s:%(asctime)s:%(message)s"
)

load_dotenv(dotenv_path='/home/taofeecoh/skylogix/.env', verbose=True)


# Define variables
base_url = 'https://api.weatherbit.io/v2.0/forecast/daily'
params = {
    'lat': '6.465422', # Lagos
    'lon': '3.406448',
    'key': os.getenv("API_KEY")
        }


# MongoDB config
MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASS = os.getenv("MONGO_PASS")
CONNECTION_STRING = f"mongodb://{MONGO_USER}:{MONGO_PASS}@localhost:27017/"


def api_to_mongo(
        url: str,
        querystrings: dict,
        connection: str,
        database: str,
        collection: str,
        source: str,
        timeout: int = 10
        ):
    """
    Function to extract data from an endpoint and store json format data.
    :param url: API endpoint
    :param querystrings: query parameters
    :param connection: MongoDB connection string
    :param database: target database
    :param collection: target collection
    :param source: data source name
    :param timeout: request timeout in seconds
    """

    try:
        response = requests.get(url, params=querystrings, timeout=timeout)
        if response.status_code == 200:
            response = response.json()
            document = {
                "source": source,
                "ingested_at": datetime.now(),
                "request": {
                    "url": url,
                    "params": querystrings
                },
                "api_response": response
            }
            logging.info("extraction complete!")
        else:
            logging.error(f"Error!: {response.text}")
    except Exception as e:
        logging.exception(f"Connection error:, {e}")

    # Initiate connection to staging database
    try:
        with MongoClient(connection) as client:
            db_collection = client[database][collection]
            db_collection.insert_one(document)

        logging.info(
            f"Successfully ingested data from"
            f"{source} into {database}.{collection}"
        )

    except PyMongoError as e:
        logging.error(f"MongoDB insertion failed: {e}")
        raise
