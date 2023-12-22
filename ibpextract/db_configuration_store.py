import logging
import mysql.connector
from mysql.connector import Error


logger = logging.getLogger(__name__)

MetadataDatabaseName = "ETLMetadata"


class DBConfigurationStore(object):

    def __init__(self, db_conf):
        self.__config = db_conf

    def _create_connection(self):
        try:
            logging.info("Db config is : {}".format(self.__config))
            conn = mysql.connector.connect(
                host=self.__config['server'],
                user=self.__config['username'],
                password=self.__config['password'],
                database=self.__config['database']
            )

            if conn.is_connected():
                logging.info("Connected to CloudSQL!")
                return conn
        except Error as e:
            logging.error("CloudSQL Connection Error! Exception: {}".format(e))

    def get_data(self, query):
        try:
            conn = self._create_connection()
            cursor = conn.cursor()
            logging.info("Execute query: {}".format(query))
            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
        except Error as e:
            logging.error("CloudSQL execution Error! Exception: {}".format(e))
        return rows

   