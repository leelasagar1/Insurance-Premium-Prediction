import os
from pymongo.mongo_client import MongoClient
from dataclasses import dataclass

@dataclass
class EnvironmentVariable:
    mongo_db_url = os.getenv('MONGO_DB_URL')

env_var = EnvironmentVariable()

mongo_client = MongoClient(env_var.mongo_db_url)

TARGET_COLUMN = 'expenses'
# Create a new client and connect to the server
# client = MongoClient(uri)