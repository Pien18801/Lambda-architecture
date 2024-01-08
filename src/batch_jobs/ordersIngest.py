from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from py4j.java_gateway import java_import


spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("IngestOrders") \
        .config("spark.jars.packages","mysql:mysql-connector-java:8.0.13") \
        .getOrCreate()

orders_mysql = spark.read.format("jdbc")\
                        .option("driver","com.mysql.cj.jdbc.Driver")\
                        .option("url", "jdbc:mysql://mysql:3306/myCompany")\
                        .option("dbtable", "Orders")\
                        .option("user", "root")\
                        .option("password", "debezium")\
                        .load()


# Import the required Java classes
java_import(spark._jvm, 'org.apache.hadoop.fs.Path')
spark._jsc.hadoopConfiguration().set("fs.defaultFS", "hdfs://namenode:9000")
filePath = "/user/root/data/orders"
# Get the Hadoop Configuration from the Spark session
hadoop_conf = spark._jsc.hadoopConfiguration()

if spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf).exists(spark._jvm.org.apache.hadoop.fs.Path(filePath)):
        orderDT_hd = spark.read.parquet("hdfs://namenode:9000" + filePath)

        record_hd = orderDT_hd.agg(max("id")).collect()[0][0]
        record_mysql = orders_mysql.agg(max("id")).collect()[0][0]

        if record_mysql - record_hd > 0:
                outputDF = orders_mysql.select("*").filter(orders_mysql.id > record_hd)
                outputDF = outputDF.withColumn("created_at", lit(datetime.date(datetime.today())))
                outputDF.write.mode('append').parquet("hdfs://namenode:9000" + filePath)
        else:
                spark.stop()
else:
        
        orders_mysql = orders_mysql.withColumn("created_at", lit(datetime.date(datetime.today())))
        orders_mysql.write.mode('append').parquet("hdfs://namenode:9000" + filePath)



