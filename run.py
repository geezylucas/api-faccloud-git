import os
import configparser
from flask import Flask
from flask_cors import CORS
from pymongo import MongoClient
from flask_apscheduler import APScheduler

app = Flask(__name__, instance_relative_config=True)
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()

config = configparser.ConfigParser()
config.read(os.path.abspath(os.path.join(".ini")))

CORS(app, resources={r"/*": {"origins": "*"}})

app.config['DEBUG'] = True
app.config['FACCLOUD_DB_URI'] = config['PROD']['FACCLOUD_DB_URI']
app.config['FACCLOUD_NS'] = config['PROD']['FACCLOUD_NS']
app.config['SECRET_KEY'] = config['PROD']['SECRET_KEY']


if __name__ == "__main__":
    from resources import test, sat_informations, requests_cfdis, cfdis
    app.register_blueprint(test.bp, url_prefix='/api/test')
    app.register_blueprint(sat_informations.bp,
                           url_prefix='/api/satinformations')
    app.register_blueprint(requests_cfdis.bp,
                           url_prefix='/api/requestscfdis')
    app.register_blueprint(cfdis.bp, url_prefix='/api/cfdis')

    app.run(host='0.0.0.0')
