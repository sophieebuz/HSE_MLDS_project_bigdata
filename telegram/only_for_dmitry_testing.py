from pyspark.sql import SparkSession
from getpass import getpass
from mysql.connector import connect, Error
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType, BooleanType, StringType
import pathlib
import re

import json


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


@udf(returnType=BooleanType())
def tickets_appeared(tickets_before, tickets_after):
    before_cond = tickets_before is None or tickets_before == 0
    after_cond = tickets_after is not None and tickets_after > 0

    return before_cond and after_cond


@udf(returnType=StringType())
def tickets_appeared_message(tickets_appeared):
    return 'Появились билеты на спектакль' if tickets_appeared else None


@udf(returnType=BooleanType())
def few_tickets(tickets_before, tickets_after, min_tickets=10):
    before_cond = tickets_before is not None and tickets_before > min_tickets
    after_cond = tickets_after is not None and tickets_after <= min_tickets

    return before_cond and after_cond


@udf(returnType=StringType())
def few_tickets_message(few_tickets, tickets_num):
    return f'Осталось мало билетов: ${tickets_num}' if few_tickets else None

'''
spark = SparkSession.builder\
        .master("local[1]")\
        .appName("Bolshoi_Theatre")\
        .config("spark.jars", "/usr/share/java/mysql-connector-java-8.2.0.jar")\
        .getOrCreate()

df = spark\
            .read\
            .format('jdbc')\
            .option('driver', 'com.mysql.cj.jdbc.Driver')\
            .option('url', 'jdbc:mysql://localhost:3306/hse')\
            .option('dbtable', 'performances')\
            .option('user', 'arhimag')\
            .option('password', 'password57')\
            .load()
'''

mock_before = {
    'date': ['12 января', '13 января', '13 января', '14 января', '14 января'],\
    'day_of_week': ['суббота', 'суббота', 'суббота', 'воскресенье', 'воскресенье'],\
    'type': ['Концерт', 'Концерт', 'Балет', 'Балет', 'Балет'],\
    'name': ['Щелкунчик', 'Щелкунчик', 'Чайка', 'Чайка', 'Чайка'],\
    'age': ['12+', '12+', '16+', '16+', '16+'],\
    'time': ['19:00', '19:00', '12:00', '12:00', '13:00'],\
    'scene': ['Историческая сцена', 'Историческая сцена', 'Новая сцена', 'Новая сцена', 'Новая сцена'],\
    'tickets': ['Билетов нет', 'Билетов нет', 'Билетов нет', '175 билетов', '10 билетов'],\
    'price': [None, None, None, 'от  7000 до 10000 ₽', 'от  7000 до 10000 ₽']
}

mock_after = {
    'date': ['13 января', '13 января', '13 января', '14 января', '14 января', '15 января'],\
    'day_of_week': ['суббота', 'суббота', 'суббота', 'воскресенье', 'воскресенье', 'понедельник'],\
    'type': ['Концерт', 'Балет', 'Балет', 'Балет', 'Балет', 'Балет'],\
    'name': ['Щелкунчик', 'Чайка', 'Чайка', 'Чайка', 'Чайка', 'Чайка'],\
    'age': ['12+', '16+', '16+', '16+', '16+', '16+'],\
    'time': ['19:00', '12:00', '16:00', '12:00', '13:00', '12:00'],\
    'scene': ['Историческая сцена', 'Новая сцена', 'Новая сцена', 'Новая сцена', 'Новая сцена', 'Новая сцена'],\
    'tickets': ['Билетов нет', '177 билетов', '177 билетов', '10 билетов', '5 билетов', '177 билетов'],\
    'price': [None, 'от  7000 до 10000 ₽', 'от  7000 до 10000 ₽', 'от  7000 до 10000 ₽', 'от  7000 до 10000 ₽', 'от  7000 до 10000 ₽']
}
'''
before_perf = spark.createDataFrame(pd.DataFrame(mock_before)).withColumn('tickets_num', tickets_num(col("tickets")))\
        .withColumn('min_price', min_price(col("price")))\
        .withColumn('max_price', max_price(col("price")))

after_perf = spark.createDataFrame(pd.DataFrame(mock_after)).withColumn('tickets_num', tickets_num(col("tickets")))\
        .withColumn('min_price', min_price(col("price")))\
        .withColumn('max_price', max_price(col("price")))

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

notifications.show()
'''
try:
    with connect(
        host="localhost",
        user='arhimag',
        password='password57',
        database='hse'
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            result = cursor.fetchall()
            for row in result:
                print(row)

            cursor.execute("describe bth_performances")
            result = cursor.fetchall()
            for row in result:
                print(row)

            cursor.execute("describe bth_notifications")
            result = cursor.fetchall()
            for row in result:
                print(row)


except Error as e:
    print(e)

