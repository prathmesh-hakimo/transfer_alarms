import mysql.connector
from mysql.connector import Error

def connect_to_database(config):
    try:
        connection = mysql.connector.connect(**config)
        if connection.is_connected():
            print("Connected to:", config['database'])
            return connection
    except Error as e:
        print(f"Connection error: {e}")
        return None 