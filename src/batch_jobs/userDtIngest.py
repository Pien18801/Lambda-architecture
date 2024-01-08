from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from py4j.java_gateway import java_import



spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("IngestUserDetail") \
        .config("spark.jars.packages","mysql:mysql-connector-java:8.0.13") \
        .getOrCreate()


userDT_mysql = spark.read.format("jdbc")\
                        .option("driver","com.mysql.cj.jdbc.Driver")\
                        .option("url", "jdbc:mysql://mysql:3306/myCompany")\
                        .option("dbtable", "User_detail")\
                        .option("user", "root")\
                        .option("password", "debezium")\
                        .load()


# Import the required Java classes
java_import(spark._jvm, 'org.apache.hadoop.fs.Path')
spark._jsc.hadoopConfiguration().set("fs.defaultFS", "hdfs://namenode:9000")
filePath = "/user/root/data/userDetail"
# Get the Hadoop Configuration from the Spark session
hadoop_conf = spark._jsc.hadoopConfiguration()

if spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf).exists(spark._jvm.org.apache.hadoop.fs.Path(filePath)):
        orderDT_hd = spark.read.parquet("hdfs://namenode:9000" + filePath)

        record_hd = orderDT_hd.agg(max("id")).collect()[0][0]
        record_mysql = userDT_mysql.agg(max("id")).collect()[0][0]

        if record_mysql - record_hd > 0:
                outputDF = userDT_mysql.select("*").filter(userDT_mysql.id > record_hd)
                outputDF.write.mode('append').parquet("hdfs://namenode:9000" + filePath)
        else:
                spark.stop()
else:
        userDT_mysql.write.mode('append').parquet("hdfs://namenode:9000" + filePath)
