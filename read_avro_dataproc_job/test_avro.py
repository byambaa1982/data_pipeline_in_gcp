from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from google.cloud import storage
import sys

from avro.datafile import DataFileReader
from avro.io import DatumReader
import json

os.environ["GCLOUD_PROJECT"] = "twittersheet-275317"
project_id='twittersheet-275317'
destination_bucket='getting-termites-tweet'

client = storage.Client()

spark = SparkSession.builder.appName('test').getOrCreate()
# df = spark.read.format("avro").load("gs://getting-termites-tweet/example_v2.avro")
# print(df.shape)
# df.toPandas().to_csv('mycsv.csv')
print('it works')
# print("byamba aaaaaaaaaaaaaaa")