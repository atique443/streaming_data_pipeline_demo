import time
from os import environ
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from data_aggregator import DataAggregator

print("Waiting for the data generator...")
sleep(20)
print("ETL Starting...")

while True:
    try:
        psql_engine = create_engine(
            environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10
        )
        break
    except OperationalError:
        sleep(0.1)
print("Connection to PostgresSQL successful.")
# connecting to mysql
while True:
    try:
        mysql_engine = create_engine(
            environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10
        )
        break
    except OperationalError:
        sleep(0.1)
print("Connection to PostgresSQL successful.")

# Write the solution here

data_aggregator = DataAggregator(psql_engine, mysql_engine)
data_aggregator.create_analysis_table()

while True:
    data_aggregator.perform_hourly_analysis()
