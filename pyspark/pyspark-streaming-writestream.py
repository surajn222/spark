import sys

# need to import for session creation
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import to_json, struct
import configparser

# creating the spark session
spark = SparkSession.builder.master("local").appName("SparkStructuredReadStream").getOrCreate()

config = configparser.ConfigParser()
config.read('config.ini')

bootstrap_servers = config['write']['bootstrap_servers']
topic = config['write']['topic']

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

#df.select(to_json(struct([df[x] for x in df.columns])).alias("value")).write.outputMode("update").format("console").save()
df.select(to_json(struct([df[x] for x in df.columns])).alias("value")).write.format("kafka").option("kafka.bootstrap.servers", bootstrap_servers).option("checkpointLocation","hdfs:///tmp").option("topic", topic).save()

sys.exit()



df2 = df.select("name", "gender").rdd.collectAsMap()
df2.show()

df.writeStream.outputMode("update").format("console").start().awaitTermination()

sys.exit()




qry = df.writeStream.format("console").option("truncate","false").start()
qry.awaitTermination()

sys.exit()

