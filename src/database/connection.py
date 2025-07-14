import mysql.connector
from mysql.connector import Error
from src.utils.logger import get_logger
logger = get_logger("db_connection")

def connect_to_database(config):
    try:
        connection = mysql.connector.connect(**config)
        if connection.is_connected():
            logger.info(f"Connected to: {config['database']}, user: {config['user']}, host: {config['host']}")
            return connection
    except Error as e:
        logger.error(f"Connection error: {e}")
        return None 