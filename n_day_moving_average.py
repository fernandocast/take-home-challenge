import argparse
from argparse import Namespace

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *


class NDayMovingAverageApp:
    def __init__(self, args: Namespace):
        self.n_day = int(args.n_day)
        self.input = args.input
        self.output = args.output
        self.spark = SparkSession.builder.appName("N-day-MA").getOrCreate()

    def get_start_boundary(self, n_days: int) -> int:
        return n_days*-1+1

    def read_historical_data(self, path: str) -> DataFrame:
        return self.spark.read. \
            option("header", True). \
            option("inferSchema", True). \
            csv(path)

    def get_moving_avg(self, df: DataFrame) -> DataFrame:

        start_boundary = self.get_start_boundary(self.n_day)

        window_spec = Window.partitionBy("ticker").orderBy("date").rowsBetween(start_boundary, 0)

        return df.withColumn("moving_avg", avg(col("close")).over(window_spec))

    def write_moving_avg(self, df: DataFrame) -> None:
        df.coalesce(1).write.mode("overwrite").parquet(self.output)


    def calculation(self):
        hist_stock_prices_df = self.read_historical_data(self.input)
        moving_avg_df =  self.get_moving_avg(hist_stock_prices_df)
        self.write_moving_avg(moving_avg_df)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n_day", help="number of days for window")
    parser.add_argument("--input", help="input path")
    parser.add_argument("--output", help="output path")
    args = parser.parse_args()
    NDayMovingAverageApp(args).calculation()


if __name__ == '__main__':
    main()
