from flask import current_app, g
from pymongo import MongoClient
from werkzeug.local import LocalProxy


def get_db():
    """
    Configuration method to return db instance
    """
    db = getattr(g, "_database", None)
    FACCLOUD_DB_URI = current_app.config["FACCLOUD_DB_URI"]
    FACCLOUD_DB_NAME = current_app.config["FACCLOUD_NS"]

    if db is None:
        db = g._database = MongoClient(FACCLOUD_DB_URI)[FACCLOUD_DB_NAME]
    return db


# Use LocalProxy to read the global db instance with just `db`
db = LocalProxy(get_db)
