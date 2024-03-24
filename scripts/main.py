import os
import argparse
import pytz

from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from pyspark.sql import DataFrame as SparkDataFrame

spark = (SparkSession.builder.appName('main').master(
    'spark://spark:7077').config(
    "spark.jars.packages", "org.postgresql:postgresql:42.7.3").getOrCreate())
spark.sparkContext.setLogLevel("INFO")

log4j = spark._jvm.org.apache.log4j
logger = log4j.LogManager.getLogger(__name__)

CURRENT_DATETIME = datetime.now(pytz.timezone('Asia/Bangkok'))
SILVER_ZONE_DIR: str = "silver"


def extract(source_file_path: str):
    logger.info("start extract...")
    return spark.read.csv(source_file_path, sep='|', header=True, inferSchema=True)


def transform_fav_product(tmp_df: SparkDataFrame):
    logger.info("start transform favorite product...")
    df2 = tmp_df.groupBy('custId', 'productSold').agg(F.sum('unitsSold').alias('totalUnitsSold')).orderBy('custId')

    w = W.partitionBy('custId').orderBy(F.col("totalUnitsSold").desc())
    df3 = df2.withColumn("row", F.row_number().over(w)).filter(F.col("row") == 1).drop("row")
    df3 = df3.withColumnRenamed('productSold', 'favourite_product').drop("totalUnitsSold")
    return df3.alias('fav_df')


def transform_longest_streak(tmp_df: SparkDataFrame):
    logger.info("start transform longest streak...")
    df2 = tmp_df.groupby(['custId', 'transactionDate']).agg(F.count('transactionId').alias('trxCount'))
    w = W.partitionBy('custId').orderBy('transactionDate')
    df2 = df2.withColumn('transactionDate', F.to_date('transactionDate', format='yyyy-MM-dd')).orderBy(
        ["custId", "transactionDate"])
    diff = F.datediff('transactionDate', F.lag('transactionDate').over(w))
    df3 = df2.withColumn('datediff', F.coalesce(diff))
    df3 = df3.withColumn('datediff', F.coalesce(diff, F.lit(0)))
    df3 = df3.withColumn('isConsecutive', F.when(F.col('datediff') == 1, True).otherwise(False))
    df3 = df3.withColumn('group', F.sum((~F.col('isConsecutive')).cast('int')).over(w))
    df4 = df3.groupBy('custID', 'group').agg(F.sum(F.lit(1)).alias('longestStreak'))

    return df4.groupBy('custID').agg(F.max('longestStreak').alias('longest_streak'))


def transform_summarize(df1: SparkDataFrame, df2: SparkDataFrame, join_keys: str = None, order_keys: str = None):
    logger.info("start transform merge...")
    if order_keys:
        return df1.join(df2, join_keys).orderBy(order_keys)
    else:
        return df1.join(df2, join_keys)


def load(df: SparkDataFrame, destination_file_path: str):
    logger.info("start load...")

    year = CURRENT_DATETIME.year
    month = CURRENT_DATETIME.month
    day = CURRENT_DATETIME.day
    partition_dir = "year={}/month={}/day={}/".format(year, month, day)
    path = os.path.join(destination_file_path, partition_dir)
    logger.info(f"------------write_path: {path}")

    ###
    # write to storage path
    ###
    df.write.option("header", "true").mode('overwrite').csv(path)

    ###
    # incase to load to db
    ###
    postgres_url = "jdbc:postgresql://postgres:5432/warehouse"
    postgres_properties = {
        "user": "airflow",
        "password": "airflow",
        "driver": "org.postgresql.Driver"
    }

    df.write.format("jdbc") \
        .option("url", postgres_url) \
        .option("dbtable", "customers") \
        .option("user", postgres_properties["user"]) \
        .option("password", postgres_properties["password"]) \
        .option("driver", postgres_properties["driver"]) \
        .mode("append") \
        .save()


def main(source: str, destination: str):
    df = extract(source)
    tmp_df = df.alias('tmp_df')

    fav_product_df = transform_fav_product(tmp_df)
    streak_df = transform_longest_streak(tmp_df)
    final_df = transform_summarize(fav_product_df, streak_df, ['custID'], ['custID'])

    load(final_df, destination)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=str, required=True, help='datasource path')
    parser.add_argument('--destination', type=str, required=True, help='destination path')

    args = parser.parse_args()
    source_path, destination_path = args.source, args.destination
    print(f"----------------: {os.getcwd()}")
    print(f"----------------: {os.listdir()}")
    main(source_path, destination_path)
