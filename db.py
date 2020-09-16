import datetime
import decimal
import time

import pymysql

from .config import Config
from .utils.logger import logger

cfg = Config()
# map to hold db connections
db_connections_map = {}


class DbException(Exception):
    pass


def retry_mysql_operation_on_error(operation):
    """
        A mysql operations decorator that handles
        exceptions and retries the operation.
    """

    def new_operation(*args, **kwargs):
        """
            New operation function with exception handlers
        """
        retry_interval = 30
        while True:
            try:
                return operation(*args, **kwargs)
            except pymysql.OperationalError as error:
                logger.error(error)
                if "Can't connect to MySQL server" in error:
                    if retry_interval > 300:
                        retry_interval = 30
                    logger.info("Will retry connecting after {} "
                                "seconds...".format(retry_interval))
                    time.sleep(retry_interval)
                    retry_interval *= 2
                elif "Lost connection to MySQL server" in error:
                    for db in db_connections_map:
                        connection = db_connections_map[db]
                        try:
                            connection.close()
                        except Exception:
                            pass
                    db_connections_map.clear()
                else:
                    raise error

    return new_operation


@retry_mysql_operation_on_error
def get_db_connection(db):
    db_connection = db_connections_map.get(db)
    if db_connection:
        return db_connection
    logger.info("Creating new connection with db: {}".format(db))
    db_connection = pymysql.connect(
        host=cfg.MYSQL_HOST, port=int(cfg.MYSQL_PORT),
        user=cfg.MYSQL_USER, password=cfg.MYSQL_PASSWORD,
        db=db, charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor
    )
    db_connections_map[db] = db_connection
    return db_connection


def prepare_result(rows):
    result = {"rows_count": len(rows), "rows": []}
    for row in rows:
        for column in row:
            value = row[column]
            if isinstance(value, (datetime.datetime, decimal.Decimal)):
                row[column] = str(value)
    result["rows"] = rows
    return result


@retry_mysql_operation_on_error
def execute(db, query):
    db_connection = get_db_connection(db)
    db_connection.ping()  # Will raise exception if connection not up
    db_cursor = db_connection.cursor()
    logger.info(" QUERY: {}".format(query))
    db_cursor.execute(query)
    db_connection.commit()
    rows = db_cursor.fetchall()
    if not rows:
        return {}
    result = prepare_result(rows)
    return result


def get_primary_key_column(db, table):
    query = "desc {}".format(table)
    response = execute(db, query)
    rows = response["rows"]
    for row in rows:
        if row.get("Key") == "PRI":
            return row["Field"]


def select(db, table, _id, query_args, return_columns=[]):
    # Preparing select query
    select_query = "select {} from {} ".format(
        ",".join(return_columns) if return_columns else "*",
        table
    )
    if _id:
        primary_key_column = get_primary_key_column(db, table)
        select_query += " where %s='%s' " % (primary_key_column, _id)
        if query_args:
            select_query += " AND "
    # Adding arguments to query
    if query_args and _id is None:
        select_query += " where "
    select_query += " AND ".join(
        ["{}='{}'".format(key, query_args[key].replace("'", "''"))
         for key in query_args]
    )

    response = execute(db, select_query)

    if _id is not None:
        if not response:
            raise DbException("Key does not exist")
        else:
            return response["rows"][0]

    if not response:
        raise DbException("No data matched according to query")

    return response


def insert(db, table, _id, query_args, row_data):
    primary_key_column = get_primary_key_column(db, table)
    insert_query = "insert into {tbl}({clms}) values ({clm_values})".format(
        tbl=table,
        clms=(
                ("%s" % primary_key_column if _id else "") +
                ("," if _id and row_data else "") +
                (",".join([column for column in row_data]))
        ),
        clm_values=(
                ("'%s'" % _id if _id is not None else "") +
                ("," if _id and row_data else "") +
                (",".join(["'%s'" % row_data[key].replace("'", "''")
                           for key in row_data]))
        )
    )
    execute(db, insert_query)
    response = select(db, table, _id, row_data)
    return response


def update(db, table, _id, query_args, row_data):
    primary_key_column = get_primary_key_column(db, table)

    update_query = "update {} set {}"
    update_query += " where {} " if ((_id is not None) or (query_args)) else ""
    if _id is not None and query_args:
        update_query = "update {} set {} where {} AND {}"

    set_columns = ",".join(["{}='{}'".format(key, row_data[key].replace("'", "''"))
                            for key in row_data])

    main_query_column = "{}='{}'".format(primary_key_column, _id)
    query_columns = " AND ".join(["{}='{}'".format(key, query_args[key])
                                  for key in query_args])

    if _id is not None:
        if query_args:
            update_query = update_query.format(
                table, set_columns,
                main_query_column, query_columns
            )
        else:
            update_query = update_query.format(
                table, set_columns,
                main_query_column
            )

    elif query_args:
        update_query = update_query.format(table, set_columns, query_columns)

    else:
        update_query = update_query.format(table, set_columns)

    response = execute(db, update_query)
    return response


def delete(db, table, _id, query_args):
    primary_key_column = get_primary_key_column(db, table)
    delete_query = "delete from {} where {} " + ("AND {}" if query_args else "")
    main_query_column = "{}='{}'".format(primary_key_column, _id)
    query_columns = " AND ".join(["{}='{}'".format(key, query_args[key])
                                  for key in query_args])
    delete_query = delete_query.format(table, main_query_column, query_columns)
    response = execute(db, delete_query)
    return response
