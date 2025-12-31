import logging
import os
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from pymongo import MongoClient
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
    'lat': '6.465422',  # Lagos
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
        location: list,
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
    :param location: list of cities' lat and long
    :param connection: MongoDB connection string
    :param database: target database
    :param collection: target collection
    :param source: data source name
    :param timeout: request timeout in seconds
    """
    try:
        # Initiate connection to staging database
        with MongoClient(connection) as client:
            db_collection = client[database][collection]

        # Fetch data specific to each region's API
            for city in location:
                    query = {
                        **querystrings,
                        "lat": city["lat"],
                        "lon": city["lon"]
                    }

                    try:
                        response = requests.get(url, params=query, timeout=timeout)
                        if response.status_code == 200:
                            response = response.json()
                            document = {
                                "source": source,
                                "city": city["city"],
                                "ingested_at": datetime.now(tz=timezone.utc),
                                "request": {
                                    "url": url,
                                    "params": query
                                },
                                "api_response": response
                            }

                            logging.info(f"extraction complete!, now ingesting to database: {database}")

                            observation_time = datetime.fromtimestamp(response["data"][0]["ts"], tz=timezone.utc)
                            filter_query = {
                                "source": source,
                                "city": city["city"],
                                "observation_time": observation_time
                            }

                            update_query = {
                                "$set": document
                            }

                            db_collection.update_one(
                                filter_query,
                                update_query,
                                upsert=True
                            )

                            logging.info(
                            f"Weather data upserted for {city['city']} at {observation_time}"
                            )

                        else:
                            logging.error(f"Error!: {response.text}")
                    except Exception as e:
                        logging.exception(f"API Connection error:, {e}")
    except PyMongoError as e:
        logging.error(f"MongoDB insertion failed: {e}")
        raise


# print(api_to_mongo(url=base_url,
#                 querystrings=params,
#                 connection=CONNECTION_STRING,
#                 database='skylogix',
#                 collection='weather_staging',
#                 source="weatherbits.io"))
