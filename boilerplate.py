from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]")
          .appName("SparkByExamples.com")
          .getOrCreate()

#Read method 1
df = spark.read.csv("/tmp/resources/zipcodes.csv")
df.printSchema()

#Read method 2
df = spark.read.format("csv").load("/tmp/resources/zipcodes.csv")
#or
df = spark.read.format("org.apache.spark.sql.csv").load("/tmp/resources/zipcodes.csv")
df.printSchema()

#Read method 3
df2 = spark.read.option("header", True).csv("/tmp/resources/zipcodes.csv")

#Read method 4
df = spark.read.csv("path1,path2,path3")

#Read method 5
df = spark.read.csv("Folder path")

#Read method 6
df3 = spark.read.options(delimiter=',').csv("C:/apps/sparkbyexamples/src/pyspark-examples/resources/zipcodes.csv")

#Read method 7
df4 = spark.read.options(inferSchema='True',delimiter=',').csv("src/main/resources/zipcodes.csv")
df4 = spark.read.option("inferSchema",True).option("delimiter",",").csv("src/main/resources/zipcodes.csv")

#Read method 8
df3 = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("/tmp/resources/zipcodes.csv")

#Read method 9
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType

schema = StructType([
    StructField("A", IntegerType()),
    StructField("B", DoubleType()),
    StructField("C", StringType())
])

sqlContext.read.format("com.databricks.spark.csv").schema(schema).option("header", "true").option("mode", "DROPMALFORMED").load("some_input_file.csv")


#Read method 10

from pyspark.sql import SparkSession
spark = SparkSession.builder \
         .appName('SparkByExamples.com') \
         .getOrCreate()

data = [("James, A, Smith","2018","M",3000),
            ("Michael, Rose, Jones","2010","M",4000),
            ("Robert,K,Williams","2010","M",4000),
            ("Maria,Anne,Jones","2005","F",4000),
            ("Jen,Mary,Brown","2010","",-1)
            ]

columns=["name","dob_year","gender","salary"]
df=spark.createDataFrame(data,columns)
df.printSchema()












#Write method 1
df.write.csv("/tmp/spark_output/datacsv")

#Write method 2
df.write.format("csv").save("/tmp/spark_output/datacsv")

#Write method 3
df.write.option("header",true).option("delimiter","|").csv("file-path")

#Write method 4
df2.write.option("header",true).option("compression","gzip").csv("file-path")

#Write method 5
spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "awsaccesskey value")
#Replace Key with your AWS secret key (You can find this on IAM
spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "aws secretkey value")
spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
#Write to CSV file
df.write.parquet("s3a://sparkbyexamples/csv/datacsv")

#Write method 6
df.write.option("header","true").csv("hdfs://nn1home:8020/csvfile")

#Write method 7
df.write.mode(SaveMode.Overwrite).csv("/tmp/spark_output/datacsv")