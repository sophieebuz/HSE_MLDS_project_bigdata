from pyspark.sql import SparkSession


def upload_data_to_mysql(parquet_path, db_params, db_table):
    spark = SparkSession.builder\
        .master('local[*]')\
        .appName('Bolshoi_Theatre')\
        .config('spark.jars', '/usr/share/java/mysql-connector-java-8.2.0.jar')\
        .getOrCreate()

    df = spark.read.parquet(parquet_path)

    df\
        .write\
        .mode('overwrite')\
        .format('jdbc')\
        .option('driver', 'com.mysql.cj.jdbc.Driver')\
        .option('url', 'jdbc:mysql://localhost:3306/hse')\
        .option('dbtable', db_table)\
        .option('user', db_params['user'])\
        .option('password', db_params['password'])\
        .save()
