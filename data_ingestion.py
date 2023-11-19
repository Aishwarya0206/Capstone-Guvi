import pymongo
from pymongo import MongoClient

class data_ingestion:
    def __init__(self, local_host, db_name, col_name):
        self.local_host = local_host
        self.db_name = db_name
        self.col_name = col_name

    def db_creation_with_data_ingestion(self, response):
        try:
            myclient = pymongo.MongoClient(self.local_host)
            db_name = myclient[self.db_name]
            store_collection = db_name[self.col_name].insert_one(response)
            return({"status": "Success", "Collection": db_name.youtube_details})
        except Exception as e:
            return(e)

    def retrieve_data_after_ingestion(self, collection):
        try:
            cursor = collection.find({})
            return cursor
        except Exception as e:
            return(e)
    