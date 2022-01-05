#!/usr/bin/env python3
"""
This code can be run on hadoop system with kafka installed:
https://github.com/surajn222/vagrant-boxes/tree/main/Hadoop-Ubuntu-Single-Node
https://github.com/surajn222/vagrant-boxes/tree/main/Ubuntu-kafdrop

spark-submit command:
/home/hduser/spark-3.1.2-bin-hadoop2.7/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 pyspark_console.py
"""


import sys
import configparser
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# creating the spark session
spark = SparkSession.builder.master("local").appName("SparkStructuredReadStream").getOrCreate()

config = configparser.ConfigParser()
config.read('config.ini')

bootstrap_servers = config['read']['bootstrap_servers']
topic = config['read']['topic']

def main():
	df = spark.readStream.format("kafka").\
		option("kafka.bootstrap.servers", bootstrap_servers).\
		option("subscribe",topic).\
		option("startingOffsets", "earliest").\
		load().\
		select(F.col("value").cast("string"))

	df.writeStream.outputMode("update").format("console").option("truncate","false").start().awaitTermination()

if __name__ == "__main__":
	main()

sys.exit()


df.selectExpr("key", "value")
print(type(df))
#print(type(qry))

query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").writeStream.format("console").option("truncate","false").start()
query.awaitTermination()

sys.exit()

ds = df.selectExpr("CAST(value AS STRING)")
print(type(df))
print(type(ds))

rawQuery = ds.writeStream.queryName("qraw").format("memory").start()
raw = spark.sql("select * from qraw")
raw.show()

sys.exit()


df.writeStream.format("console").start()
df.show()
sys.exit()

query = df.limit(5).writeStream.foreachBatch(apply_pivot).start()

time.sleep(50)
spark.sql("select * from stream").show(20, False)
query.stop()

sys.exit()

readDF = df.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)")

df.writeStream.format("console").start()
df.show()

sys.exit()

ds = df.selectExpr("CAST(value AS STRING)")
print(type(df))
print(type(ds))

rawQuery = ds.writeStream.queryName("qraw").format("memory").start()
raw = spark.sql("select * from qraw")
raw.show()
print("Complete")


