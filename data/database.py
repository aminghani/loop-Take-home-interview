import configparser
import os

config = configparser.ConfigParser()
config.read('../config.ini')


db_config = {
    'user': "postgres",
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': "localhost",
    'database': "loop",
}