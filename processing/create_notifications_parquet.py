import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from mysql.connector import connect
from processing.notification_rules import tickets_appeared, tickets_appeared_message, few_tickets, few_tickets_message


def create_notifications_parquet(parquet_paths, db_params):
    try:
        with connect(
            host="localhost",
            user=db_params['user'],
            password=db_params['password'],
            database='hse'
        ) as connection:
            pd_before_perf = pd.read_sql('SELECT * from bth_performances', connection)
    except:
        print('First load data, notifications not needed')
        return

    spark = SparkSession.builder\
        .master('local[*]')\
        .appName('Bolshoi_Theatre')\
        .getOrCreate()

    before_perf = spark.createDataFrame(pd_before_perf)
    after_perf = spark.read.parquet(parquet_paths['performances'])

    perf_with_notifications = before_perf\
        .join(after_perf, (before_perf.date == after_perf.date) & (before_perf.time == after_perf.time) & (before_perf.name == after_perf.name), 'right')\
        .withColumn('tickets_appeared', tickets_appeared(before_perf.tickets_num, after_perf.tickets_num))\
        .withColumn('tickets_appeared_message', tickets_appeared_message(col('tickets_appeared')))\
        .withColumn('few_tickets', few_tickets(before_perf.tickets_num, after_perf.tickets_num))\
        .withColumn('few_tickets_message', few_tickets_message(col('few_tickets'), after_perf.tickets_num))\
        .select(after_perf.date, after_perf.time, after_perf.name, col('tickets_appeared'), col('few_tickets'), col('tickets_appeared_message'), col('few_tickets_message'),)\

    tickets_appeared_notifications = perf_with_notifications\
        .select(col('date'), col('time'), col('name'), col('tickets_appeared_message').alias('message'))\
        .filter(col('tickets_appeared') == True)

    few_tickets_notifications = perf_with_notifications\
        .select(col('date'), col('time'), col('name'), col('few_tickets_message').alias('message'))\
        .filter(col('few_tickets') == True)

    notifications = tickets_appeared_notifications\
        .union(few_tickets_notifications)\
        .orderBy(col('date'), col('time'))

    notifications\
        .withColumn('date', col('date').cast(StringType))\
        .withColumn('time', col('time').cast(StringType))\
        .withColumn('name', col('name').cast(StringType))\
        .withColumn('message', col('message').cast(StringType))\
        .repartition(1)\
        .write\
        .mode('overwrite')\
        .parquet(parquet_paths['notifications'])