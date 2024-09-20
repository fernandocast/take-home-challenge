import datetime
import unittest
from unittest.mock import MagicMock

from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, Row
from sparktestingbase.sqltestcase import SQLTestCase

from spark.python.src.n_day_moving_average import NDayMovingAverageApp


class NDayMovingAverageAppTest(SQLTestCase):

    def test_get_start_boundary_should_subtract_one_and_convert_to_negative(self):
        expected_value = -6
        n_days = 7
        n_day_moving_average = NDayMovingAverageApp(MagicMock())

        result = n_day_moving_average.get_start_boundary(n_days)

        self.assertEqual(expected_value, result)

    def test_get_moving_avg_should_calculate_over_window_function(self):
        expected_schema = StructType([
            StructField("ticker", StringType(), True),
            StructField("date", DateType(), True),
            StructField("close", DoubleType(), True),
            StructField("moving_avg", DoubleType(), True)
        ])
        expected_rows = [
            Row(ticker="AA", date=datetime.date(2024, 12, 17), close=7.1, moving_avg=7.1),
            Row(ticker="AA", date=datetime.date(2024, 12, 18), close=8.2, moving_avg=7.6499999999999995),
            Row(ticker="AA", date=datetime.date(2024, 12, 19), close=6.9, moving_avg=7.3999999999999995),
            Row(ticker="AA", date=datetime.date(2024, 12, 20), close=7.7, moving_avg=7.6000000000000005),
        ]
        expected_df = self.sqlCtx.createDataFrame(expected_rows, expected_schema)

        df_schema = StructType([
            StructField("ticker", StringType(), True),
            StructField("date", DateType(), True),
            StructField("close", DoubleType(), True),
        ])

        df_rows = [
            Row(ticker="AA", date=datetime.date(2024, 12, 17), close=7.1),
            Row(ticker="AA", date=datetime.date(2024, 12, 18), close=8.2),
            Row(ticker="AA", date=datetime.date(2024, 12, 19), close=6.9),
            Row(ticker="AA", date=datetime.date(2024, 12, 20), close=7.7)

        ]

        df = self.sqlCtx.createDataFrame(df_rows, df_schema)

        n_day_moving_average = NDayMovingAverageApp(MagicMock())
        n_day_moving_average.n_day = 3
        n_day_moving_average.spark = self.sqlCtx
        result = n_day_moving_average.get_moving_avg(df)

        self.assertDataFrameEqual(expected_df, result)


if __name__ == '__main__':
    unittest.main()
