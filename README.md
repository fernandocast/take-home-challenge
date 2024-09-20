# take-home-challenge

## Description

The challenge consists in computing in a distributed way the 7-day moving average for historical
stock prices using Spark.

## Getting Started

### Dependencies
```
pip install pyspark==3.5.0
pip install spark-testing-base==0.11.1
```

### Executing program

```
spark-submit --master local[*] spark/python/src/n_day_moving_average.py --n_day 7 --input /input_path/historical_stock_prices.csv --output /output_path/
```
