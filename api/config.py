import os


class Config(object):
    DEBUG = False
    SECRET_KEY = 'dev'
    FACCLOUD_DB_URI = os.environ['FACCLOUD_DB_URI']
    FACCLOUD_NS = os.environ['FACCLOUD_NS']


class ProductionConfig(Config):
    FACCLOUD_DB_URI = os.environ['FACCLOUD_DB_URI']
    FACCLOUD_NS = os.environ['FACCLOUD_NS']


class DevelopmentConfig(Config):
    DEBUG = True


class TestingConfig(Config):
    TESTING = True
