import os
from configparser import ConfigParser
import psycopg2
from sshtunnel import SSHTunnelForwarder
from typing import Dict, Any, Tuple
import pandas as pd
import time
from loguru import logger
from psycopg2 import sql

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 500)
pd.set_option('display.max_rows', 20)


def read_config(file_name: str = 'creds//db.ini', section: str = 'postgresql') -> Tuple[
    Dict[str, Any], Dict[str, Any]]:
    config = ConfigParser()
    config.read(file_name)
    ssh_param = {
        'ssh_host': config.get(section, 'ssh_host').split(':')[0],
        'ssh_port': int(config.get(section, 'ssh_host').split(':')[1]),
        'ssh_private_key': os.path.expanduser(config.get(section, 'ssh_private_key')),
        'ssh_username': config.get(section, 'ssh_username'),
        'remote_bind_address': (
            config.get(section, 'remote_bind_address').split(':')[0],
            int(config.get(section, 'remote_bind_address').split(':')[1])
        ),
    }

    db_param = {
        'dbname': config.get(section, 'db_name'),
        'user': config.get(section, 'db_user'),
        'password': config.get(section, 'db_password'),
        'host': config.get(section, 'db_host'),
    }

    return ssh_param, db_param


def create_ssh_tunnel(params: Dict[str, Any]) -> SSHTunnelForwarder:
    return SSHTunnelForwarder(**params)


def connect_to_database(params: Dict[str, Any], port: int):
    return psycopg2.connect(port=port, **params)


def execute_sql_query(connection, sql_query) -> pd.DataFrame:
    with connection.cursor() as cursor:
        cursor.execute(sql_query)
        columns = [desc[0] for desc in cursor.description]
        result = cursor.fetchall()
        return pd.DataFrame(result, columns=columns)


def timed(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        duration = "[{name}]: Time spend: {elapsed:.2f}s".format(
            name=func.__name__.upper(),
            elapsed=time.time() - start
        )
        logger.success(duration)
        return result

    return wrapper


@timed
def get_data_from_db(query_str):
    ssh_params, db_params = read_config()
    with create_ssh_tunnel(ssh_params) as server:
        db_port = server.local_bind_port
        conn = connect_to_database(db_params, db_port)
        query = sql.SQL(query_str)
        results = execute_sql_query(conn, query)
    return pd.DataFrame(results)
