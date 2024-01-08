from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F

from logger.logger import Logger

KAFKA_ENDPOINT = "kafka:9092"                         
KAFKA_TOPIC_ORDERS    = "dbserver1.myCompany.Orders"         
KAFKA_TOPIC_ORDER_DETAIL    = "dbserver1.myCompany.Order_detail"


logger = Logger('Consumer')


spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("Test") \
        .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2," +
                    "mysql:mysql-connector-java:8.0.13"
                  ) \
        .getOrCreate()

    # self._spark.sparkContext.setLogLevel("ERROR")

def orderDF():
  try:
    df = spark \
          .readStream \
          .format("kafka") \
          .option("kafka.bootstrap.servers", KAFKA_ENDPOINT) \
          .option("subscribe", KAFKA_TOPIC_ORDERS) \
          .option("startingOffsets", "latest") \
          .option("kafka.group.id", "consumer1") \
          .load()

    df = df.selectExpr("CAST(value AS STRING)")

    schema = StructType([
                StructField("created_at", StringType(), False),
                StructField("id", IntegerType(), False),
                StructField("product_id", IntegerType(), False),
                StructField("quantity", IntegerType(), False),
        ])
    
    df = df.select(F.from_json(F.col("value"), schema).alias("data")).select("data.*")
    df = df.withColumnRenamed("id","order_id").withColumn("created_at", lit(datetime.date(datetime.now()))).withColumnRenamed("created_at","order_created_at")

    # df.printSchema()
    logger.info(f"Consume topic: {KAFKA_TOPIC_ORDERS}")
  except Exception as e:
    logger.error(e)
    # print(e)

  return df



def orderDetailDF():
  try:
    df = spark \
          .readStream \
          .format("kafka") \
          .option("kafka.bootstrap.servers", KAFKA_ENDPOINT) \
          .option("subscribe", KAFKA_TOPIC_ORDER_DETAIL) \
          .option("startingOffsets", "latest") \
          .option("kafka.group.id", "consumer2") \
          .load()
      
    # df.printSchema()  

    df = df.selectExpr("CAST(value AS STRING)")

    schema = StructType([
                StructField("id", IntegerType(), False),
                StructField("order_id", IntegerType(), False),
                StructField("payment", StringType(), False),
                StructField("total", IntegerType(), False),
                StructField("user_id", IntegerType(), False),
        ])
    
    df = df.select(F.from_json(F.col("value"), schema).alias("data")).select("data.*")
    df = df.withColumnRenamed("id","detail_id")

    # df.printSchema()

    logger.info(f"Consume topic: {KAFKA_TOPIC_ORDER_DETAIL}")
  except Exception as e:
    logger.error(e)
    # print(e)

  return df



def save_to_speed_view(batch_df, batch_id):
  tableName = "speedView_" + str(datetime.now().day) + "_" + str(datetime.now().month) + "_" + str(datetime.now().year)

  mapDF = batch_df.groupBy("product_id", "Make", "Model", "Category", "payment","inv_quantity")\
                .agg(sum("quantity").alias("Sold"),\
                    sum("total").alias("Revenue"))\
                .withColumn("created_at", lit(datetime.now()))
  df = mapDF.select("product_id", "Make", "Model", "Category", "payment", "Sold", "Revenue", "inv_quantity", "created_at")

  records = df.count()

  df.write \
        .format("jdbc")\
        .option("driver","com.mysql.cj.jdbc.Driver")\
        .option("url", "jdbc:mysql://mysql_des:3306/myCompany")\
        .option("dbtable", tableName)\
        .option("user", "root")\
        .option("password", "debezium")\
        .mode("append")\
        .save()

  logger.info(f"Save to speed view: ({records} records)")   

def run():
      products_df = spark.read.format("jdbc")\
                              .option("driver","com.mysql.cj.jdbc.Driver")\
                              .option("url", "jdbc:mysql://mysql:3306/myCompany")\
                              .option("dbtable", "Products")\
                              .option("user", "root")\
                              .option("password", "debezium")\
                              .load()
      
      inventory_df = spark.read.format("jdbc")\
                              .option("driver","com.mysql.cj.jdbc.Driver")\
                              .option("url", "jdbc:mysql://mysql:3306/myCompany")\
                              .option("dbtable", "Inventory")\
                              .option("user", "root")\
                              .option("password", "debezium")\
                              .load()
      inventory_df = inventory_df.withColumnRenamed("quantity","inv_quantity")
      # .drop(order_detail_df.order_id)


      orders_df = orderDF()
      order_detail_df = orderDetailDF()
      
      preDF = orders_df.join(order_detail_df, orders_df.order_id == order_detail_df.order_id, 'inner')\
                    .join(products_df, orders_df.product_id == products_df.id, 'inner')\
                    .join(inventory_df, products_df.inventory_id == inventory_df.id, 'inner')

      preDF = preDF.select("product_id", "Make", "Model", "Category", "payment", "quantity", "total", "inv_quantity")


# .trigger(processingTime='5 seconds') \
      stream = preDF\
        .writeStream \
        .trigger(processingTime='5 seconds')\
        .foreachBatch(save_to_speed_view) \
        .outputMode("append") \
        .start() 
        
      stream.awaitTermination()


if __name__ == '__main__':
  run()