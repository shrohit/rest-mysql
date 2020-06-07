
import sys

import pymysql
from cerberus import Validator
from flask import Flask, request
from flask_restful import Resource, Api

from .config import Config
from .utils.logger import logger
from .validators import execute_query_payload_validator
from .db import (
    execute, select, insert, update,
    delete, DbException
)


cfg = Config()
# Flask App
app = Flask(__name__)
api = Api(app)
# List of available endpoints
available_endpoints = {}


class QueryExecute(Resource):

    def __validate_request_payload(self, request_payload):
        validator = Validator()
        validated = validator.validate(request_payload,
                                       execute_query_payload_validator)
        if not validated:
            logger.info("QueryExecute validation failed")
            logger.info(validator.errors)
            raise RestMysqlException(validator.errors)

    def post(self, db):
        try:
            db = db.strip()
            if not db:
                raise RestMysqlException("Please supply a DB name")
            request_payload = request.json
            if not request_payload:
                raise RestMysqlException("Received empty JSON Payload")

            self.__validate_request_payload(request_payload)
            query = request_payload["query"]
            response = execute(db, query)
            if response:
                return response, 200
            else:
                return {"status": "no-query-response"}, 200

        except (RestMysqlException, pymysql.err.Error) as err:
            logger.info(err)
            return str(err), 400

        except Exception as err:
            logger.critical(err, exc_info=1)
            return str(err), 500


class TableIO(Resource):

    def __validate_uri_params(self, db, table, id, query_args={}):
        db, table = map(unicode.strip, [db, table])
        if "" == db:
            raise RestMysqlException("Please provide db name")
        elif "" == table:
            raise RestMysqlException("Please provide table name")
        # Fixing whitespaces in id
        if id is not None:
            id = id.strip()
            if id == "":
                id = None
        return db, table, id, query_args

    def get(self, db, table, id=None):
        try:
            query_args = request.args
            db, table, id, query_args = self.__validate_uri_params(
                                        db, table, id, query_args)
            response = select(db, table, id, query_args)
            return response, 200

        except (RestMysqlException, pymysql.err.Error) as err:
            logger.info(err)
            return str(err), 400

        except DbException as err:
            logger.info(err)
            error_msg = str(err)
            return error_msg, 404

        except Exception as err:
            logger.critical(err, exc_info=1)
            return str(err), 500

    def post(self, db, table, id=None):
        try:
            query_args = request.args
            row_data = request.json
            if row_data is None:
                raise RestMysqlException("No JSON Payload Found")
            db, table, id, query_args = self.__validate_uri_params(
                                        db, table, id, query_args)
            response = insert(db, table, id, query_args, row_data)
            return response, 200

        except (RestMysqlException, pymysql.err.Error) as err:
            error_msg = str(err)
            logger.info(error_msg)
            if "Duplicate entry" in error_msg:
                # Data already exist
                return error_msg, 409
            return error_msg, 400

        except Exception as err:
            logger.critical(err, exc_info=1)
            return str(err), 500

    def put(self, db, table, id=None):
        try:
            query_args = request.args
            row_data = request.json
            if not row_data:
                raise RestMysqlException("No JSON Payload Found")
            db, table, id, query_args = self.__validate_uri_params(
                                        db, table, id, query_args)
            # Checking if row exists
            select(db, table, id, query_args)
            # Performing update
            response = update(db, table, id, query_args, row_data)
            return response, 200

        except (RestMysqlException, pymysql.err.Error) as err:
            logger.info(err)
            return str(err), 400

        except DbException as err:
            logger.info(err)
            error_msg = str(err)
            if "Key does not exist" in error_msg:
                return error_msg, 404

        except Exception as err:
            logger.critical(err, exc_info=1)
            return str(err), 500

    def delete(self, db, table, id=None):
        try:
            query_args = request.args
            if id is None:
                raise RestMysqlException("Please specify the ID")
            db, table, id, query_args = self.__validate_uri_params(
                                        db, table, id, query_args)
            # Checking if row exists
            select(db, table, id, query_args)
            # Performing delete
            response = delete(db, table, id, query_args)
            return response, 200

        except (RestMysqlException, pymysql.err.Error) as err:
            logger.info(err)
            return str(err), 400

        except DbException as err:
            logger.info(err)
            error_msg = str(err)
            if "Key does not exist" in error_msg:
                return error_msg, 404

        except Exception as err:
            logger.critical(err, exc_info=1)
            return str(err), 500


class AvailableEndpoints(Resource):
    def get(self):
        return available_endpoints


class RestMysqlException(Exception):
    pass


def update_available_endpoints(route, endpoint_name):
    logger.info("Added Endpoint: %s Route: %s" % (endpoint_name, route))
    available_endpoints[route] = endpoint_name


def setup_api_endpoints():
    routes = {
        "/<string:db>/_execute": {
            "name": "execute_direct_query_on_db",
            "resource": QueryExecute
        },
        "/<string:db>/<string:table>/": {
            "name": "table_operations",
            "resource": TableIO
        },
        "/<string:db>/<string:table>/<id>": {
            "name": "row_operations",
            "resource": TableIO
        },
        "/endpoints": {
            "name": "lists_available_endpoints",
            "resource": AvailableEndpoints
        }
    }
    for route in routes:
        endpoint_name = routes[route]["name"]
        resource_class = routes[route]["resource"]
        api.add_resource(
            resource_class,
            route,
            endpoint=endpoint_name
        )
        update_available_endpoints(route, endpoint_name)


def start_api():
    logger.info("Starting REST-MySQL...")
    # PyMySql is not thread safe therefore executing multiple queries
    # parallely in same connection can lead to undefined behaviour.
    app.run(
        host=cfg.LISTEN_IP,
        port=cfg.LISTEN_PORT,
        # Disabled multithreading for maintaining one query only at a time
        threaded=False,
        debug=False
    )


def main():
    setup_api_endpoints()
    start_api()


if __name__ == "__main__":
    try:
        main()

    except KeyboardInterrupt:
        print "Exiting..."

    except Exception as err:
        logger.critical(err, exc_info=1)
        sys.exit(1)
