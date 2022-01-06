import sys

# need to import for session creation
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# creating the spark session
spark = SparkSession.builder.master("local").appName("SparkStructuredWriteStream").getOrCreate()

data = [
		("James, A, Smith","2018","M",3000),
		("Michael, Rose, Jones","2010","M",4000),
		("Robert,K,Williams","2010","M",4000)
		]

columns=["name","dob_year","gender","salary"]
df=spark.createDataFrame(data,columns)

# show table
df.show()

# show schema
df.printSchema()

from pyspark.sql.functions import to_json, struct

(df.select(to_json(struct([df[x] for x in df.columns])).alias("value")).write.outputMode("update").format("console").save()

import sys
sys.exit()


df2 = df.select("name", "gender").rdd.collectAsMap()
df2.show()



df.write.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("checkpointLocation","hdfs:///tmp").option("topic", "topic_2").save()


df.writeStream.outputMode("update").format("console").start().awaitTermination()

import sys
sys.exit()








df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9093").option("subscribe", "topic_1").option("startingOffsets", "earliest").load()
df.selectExpr("key", "value")
print(type(df))
#print(type(qry))


qry = df.writeStream.format("console").option("truncate","false").start()
qry.awaitTermination()

sys.exit()

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "topic_1").option("startingOffsets", "earliest").load()

#df.show()
print("Printing to console")

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






def main():
	print("HW")




if __name__ == "__main__":
	main()

