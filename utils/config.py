import os

from dotenv import load_dotenv

load_dotenv(dotenv_path='/home/taofeecoh/skylogix/.env', verbose=True)

# MongoDB config
MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASS = os.getenv("MONGO_PASS")
CONNECTION_STRING = f"mongodb://{MONGO_USER}:{MONGO_PASS}@localhost:27017/"
