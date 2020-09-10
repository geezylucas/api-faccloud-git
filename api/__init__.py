import os
from flask import Flask
from flask_cors import CORS
from api import config


def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)

    # Enable CORS
    CORS(app, resources={r"/*": {"origins": "*"}})

    if test_config is None:
        # app.config.from_pyfile('config.py', silent=True)
        app.config.from_object(config.DevelopmentConfig)
    else:
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    # Register the BluePrints
    from api.resources import test, sat_informations, requests_cfdis, cfdis, users
    app.register_blueprint(test.bp, url_prefix='/api/test')
    app.register_blueprint(sat_informations.bp, url_prefix='/api/satinformations')
    app.register_blueprint(requests_cfdis.bp, url_prefix='/api/requestscfdis')
    app.register_blueprint(cfdis.bp, url_prefix='/api/cfdis')
    app.register_blueprint(users.bp, url_prefix='/api/users')

    return app
