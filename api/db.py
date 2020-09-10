from flask import current_app, g
from pymongo import MongoClient


def get_db():
    """
    Configuration method to return db instance
    """
    db = getattr(g, "_database", None)
    faccloud_db_uri = current_app.config["FACCLOUD_DB_URI"]
    faccloud_db_name = current_app.config["FACCLOUD_NS"]

    if db is None:
        db = g._database = MongoClient(faccloud_db_uri)[faccloud_db_name]
    return db
