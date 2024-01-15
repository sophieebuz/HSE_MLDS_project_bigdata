from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType
import pathlib
import re


@udf(returnType=IntegerType())
def tickets_num(ticket_str):
    if ticket_str is not None:
        ticket_num = re.findall('\d+', ticket_str)

        return int(ticket_num[0]) if len(ticket_num) > 0 else None


@udf(returnType=IntegerType())
def max_price(price_str):
    if price_str is not None:
        prices = re.findall('\d+', price_str)

        if len(prices) == 2:
            return int(prices[1])

        return int(prices[0]) if len(prices) > 0 else None


@udf(returnType=IntegerType())
def min_price(price_str):
    if price_str is not None:
        prices = re.findall('\d+', price_str)

        return int(prices[0]) if len(prices) > 0 else None


def create_performances_parquet(parquet_paths):
    spark = SparkSession.builder\
        .master("local[*]")\
        .appName('Bolshoi_Theatre')\
        .getOrCreate()

    df_performances = spark.read.parquet(parquet_paths['parsing'])

    df_performances\
        .withColumn('tickets_num', tickets_num(col("tickets")))\
        .withColumn('min_price', min_price(col("price")))\
        .withColumn('max_price', max_price(col("price")))\
        .repartition(1)\
        .write\
        .mode('overwrite')\
        .parquet(parquet_paths['performances'])
