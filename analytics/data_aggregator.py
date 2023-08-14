"""This class contains the data aggregation operations.

This class contains the data aggregation operations.

    - Max temperature by device id per hour
    - The amount of data points by device id per hour
    - Total distance travelled by device id per hour

Connected instance of sqlalchemy engine is required to perform the operations.
"""
import datetime
import json
import math
from time import sleep

import pandas as pd
from sqlalchemy import (
    MetaData,
    Column,
    Table,
    String,
    Integer,
    Float,
    text,
)


class DataAggregator:
    """Define the data aggregator class.

    This class connects to the database
    and performs the data aggregation operations,
    using the sqlalchemy ORM.

    The class generates the aggregations per device id per hour.
    """

    def __init__(self, postgres_engine, mysql_engine):
        self.postgres_engine = postgres_engine
        self.mysql_engine = mysql_engine
        self.analysis_table = None

    # def init_mysql_connection(self):
    #     # Initialize MySQL connection using the provided engine URL
    #     self.mysql_engine = create_engine(mysql_engine_url)

    def create_analysis_table(self):
        metadata_obj = MetaData()
        self.analysis_table = Table(
            "hourly_analysis",
            metadata_obj,
            Column("device_id", String(100)),
            Column("max_temperature", Integer),
            Column("total_distance", Float),
            Column("data_points", Integer),
            Column("hour_start", Integer),
        )
        metadata_obj.create_all(self.mysql_engine)

    def pull_raw_data(self):
        """Pull data from the db."""
        with self.postgres_engine.connect() as postgres_conn:
            min_seconds_query = text("SELECT MIN(time) FROM devices")
            result = postgres_conn.execute(min_seconds_query)
            min_seconds = result.scalar()
            max_seconds = int(min_seconds) + 3600

            with self.mysql_engine.connect() as mysql_conn:
                last_hour_start_query = text(
                    "SELECT MAX(hour_start) FROM hourly_analysis"
                )
                last_hour_start_result = mysql_conn.execute(last_hour_start_query)
                last_hour_start = last_hour_start_result.scalar()

                if last_hour_start:
                    min_seconds = last_hour_start
                    max_seconds = last_hour_start + 3600

                analysis_query = text(
                    f"""
                    SELECT
                        *
                    FROM devices
                    WHERE time >= {min_seconds}
                        AND time < {max_seconds}
                """
                )
                analysis_result = postgres_conn.execute(analysis_query)

                # Convert analysis_result to a Pandas DataFrame
                analysis_df = pd.DataFrame(analysis_result)
        return analysis_df, max_seconds

    def perform_hourly_analysis(self):
        """Perform the hourly analysis."""
        analysis_df, hour_start = self.pull_raw_data()
        if analysis_df.empty:
            print("No new data points to analyze.")
            print("Sleeping for 1 hour...")
            sleep(3600)  # Sleep for 1 hour
            self.perform_hourly_analysis()
        else:
            print("Analyzing new data points...")
            # Group the data by device_id
            grouped_data = analysis_df.groupby("device_id")

            # Calculate the max temperature per device_id
            max_temperature = grouped_data["temperature"].max()

            # Calculate the total distance travelled per device_id
            total_distance = grouped_data["location"].apply(
                lambda x: self.calculate_total_distance(x)
            )

            # Calculate the amount of data points per device_id
            data_points = grouped_data["device_id"].count()

            # Combine the series into a single DataFrame
            analysis_df = pd.concat(
                [max_temperature, total_distance, data_points], axis=1
            )
            timestamp = datetime.datetime.fromtimestamp(hour_start)
            # round off to the nearest hour start
            timestamp = timestamp.replace(microsecond=0, second=0, minute=0)
            # add the hour start column
            analysis_df["hour_start"] = int(timestamp.timestamp())
            # Rename the columns
            analysis_df.columns = [
                "max_temperature",
                "total_distance",
                "data_points",
                "hour_start",
            ]

            # Reset the index
            analysis_df = analysis_df.reset_index()

            # Convert the DataFrame to a list of dictionaries
            analysis_dicts = analysis_df.to_dict(orient="records")

            # Insert the analysis results into the MySQL database
            with self.mysql_engine.connect() as mysql_conn:
                for analysis_dict in analysis_dicts:
                    mysql_conn.execute(self.analysis_table.insert(), analysis_dict)
                    mysql_conn.commit()
            print("Analysis complete. selecting next window")

    # Function to calculate the haversine distance
    def calculate_total_distance(self, data):
        # Calculate distance traveled
        total_distance = 0
        data_ = data.reset_index(drop=True)
        for i, r in data_.iteritems():
            current_row_dict = {"latitude": 0, "longitude": 0}
            next_row_dict = {"latitude": 0, "longitude": 0}

            try:
                current_row_dict = json.loads(r)
            except:
                pass
            try:
                next_row_dict = json.loads(data_[i + 1])
            except:
                pass
            lat1, lon1 = float(current_row_dict["latitude"]), float(
                current_row_dict["longitude"]
            )
            lat2, lon2 = float(next_row_dict["latitude"]), float(
                next_row_dict["longitude"]
            )
            total_distance += self.haversine_distance(lat1, lon1, lat2, lon2)
        return total_distance

    @staticmethod
    def haversine_distance(lat1, lon1, lat2, lon2):
        R = 6371  # Earth radius in kilometers

        # Convert latitude and longitude from degrees to radians
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance = R * c

        return distance
