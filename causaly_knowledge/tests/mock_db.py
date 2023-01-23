from unittest import TestCase
import mysql.connector
from mysql.connector import errorcode
# from mock import patch
# import utils
import config_module
import os

config_file_path = f"{os.path.dirname(__file__)}/test_config.yaml"


class MockDB(TestCase):
    def __init__(self, db_name):
        self.db_name = db_name

        config_module.load_config(config_file_path)
        self.server_credentials = config_module.config['server_credentials']
        

    def setUpClass(self):
        connection = mysql.connector.connect(
            user=self.server_credentials['user'],
            password=self.server_credentials['password']
        )
        cursor = connection.cursor(dictionary=True)

        # drop database if it already exists
        try:
            cursor.execute(f"DROP DATABASE {self.db_name}")
            cursor.close()
            print("DB dropped")
        except mysql.connector.Error as err:
            print("{}{}".format(self.db_name, err))

        cursor = connection.cursor(dictionary=True)
        try:
            cursor.execute(f"CREATE DATABASE {self.db_name}")
        except mysql.connector.Error as err:
            print("Failed creating database: {}".format(err))
            exit(1)
        # connection.database = self.db_name

        query = f"""CREATE TABLE {self.db_name}.test_table (
                  article_uuid varchar(255) PRIMARY KEY,
                  article_name varchar(255),
                  article_ID int,
                  journal_name varchar(255),
                  citation_status varchar(255),
                  citation_version varchar(255),
                  operation_type varchar(255)
                )"""
        try:
            cursor.execute(query)
            connection.commit()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("test_table already exists.")
            else:
                print(err.msg)
        else:
            print("test_table created")

   
    def tearDownClass(self):
        connection = mysql.connector.connect(
            database=self.db_name,
            user=self.server_credentials['user'],
            password=self.server_credentials['password']
        )
        cursor = connection.cursor(dictionary=True)

        # drop test database
        try:
            cursor.execute(f"DROP DATABASE {self.db_name}")
            connection.commit()
            cursor.close()
        except mysql.connector.Error as err:
            print(f"Database {self.db_name} does not exists. Dropping db failed")
        connection.close()