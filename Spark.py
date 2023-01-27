import logging
import multiprocessing
import os
# findspark.init()
# import org.apache.spark.sql.cassandra._
from json import dumps
import prestodb
import boto3
import pandas as pd
import pyspark
from pyspark.sql import SparkSession


try:

    s3 = boto3.resource('s3')
    bucket = s3.Bucket('simeon-streaming-bucket')

    os.environ[
        'PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.1.0 ' \
                                 'spark_s3_cassandra.py pyspark-shell '

    spark = SparkSession.builder.master("local").appName("testapp").getOrCreate()
    sc = spark.sparkContext

    json_list = []

    for i in range(10):
        obj = s3.Object(bucket_name='simeon-streaming-bucket', key=f'api_data{i}.json').get()
        obj_string_to_json = obj["Body"].read().decode('utf-8')
        data = dumps(obj_string_to_json).replace("'", '"').rstrip('"').lstrip('"')
        json_list.append(data)

    df = spark.read.option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record").json(
        sc.parallelize(json_list))

    type(df)
    df.show(truncate=True)

    df.write.format("org.apache.spark.sql.cassandra").mode("overwrite").option("confirm.truncate", "true").option(
        "keyspace", "api_data").option("table", "pinterest_data").save()

    spark.stop()

    connection = prestodb.dbapi.connect(
        host='localhost',
        catalog='cassandra',
        user='Simeon',
        port=8080,
        schema='api_data'
    )

    cur = connection.cursor()
    cur.execute("SELECT * FROM pinterest_data")
    rows = cur.fetchall()

    api_df = pd.DataFrame(rows)
    print(api_df)


except Exception as e:
    logging.basicConfig(filename="/home/ubuntu/airflow/error_log",
                        filemode='a',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.ERROR)
    logging.error(e, exc_info=True)


cfg = (
    pyspark.SparkConf()
    # Setting the master to run locally and with the maximum amount of cpu coresfor multiprocessing.
    .setMaster(f"local[{multiprocessing.cpu_count()}]")
    # Setting application name
    .setAppName("TestApp")
    # Setting config value via string
    .set("spark.eventLog.enabled", False)
    # Setting environment variables for executors to use
    .setExecutorEnv(pairs=[("VAR3", "value3"), ("VAR4", "value4")])
    # Setting memory if this setting was not set previously
    .setIfMissing("spark.executor.memory", "1g")
)

# Getting a single variable
print(cfg.get("spark.executor.memory"))
# Listing all of them in string readable format
print(cfg.toDebugString())

session = pyspark.sql.SparkSession.builder.config(conf=cfg).getOrCreate()

rddDistributedFile = session.sparkContext.textFile("*.json")

rddDistributedFile = rddDistributedFile.cache()
pyspark.StorageLevel.DISK_ONLY