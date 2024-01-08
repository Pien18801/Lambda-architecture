from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from logger.logger import Logger

logger = Logger('Transform')

spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("Transform") \
        .config("spark.jars.packages","mysql:mysql-connector-java:8.0.13") \
        .getOrCreate()


inventoryDF = spark.read.parquet("hdfs://namenode:9000/user/root/data/inventory")
productsDF = spark.read.parquet("hdfs://namenode:9000/user/root/data/products")
ordersDF = spark.read.parquet("hdfs://namenode:9000/user/root/data/orders")
orderDetailDF = spark.read.parquet("hdfs://namenode:9000/user/root/data/orderDetail")
usersDF = spark.read.parquet("hdfs://namenode:9000/user/root/data/users")
userDetailDF = spark.read.parquet("hdfs://namenode:9000/user/root/data/userDetail")



tmpProductsDF = productsDF.join(inventoryDF, productsDF.inventory_id == inventoryDF.id, "inner").drop(inventoryDF.id)
tmpProductsDF = tmpProductsDF.withColumnRenamed("quantity", "inv_quantity")
# tmpProductsDF.show(5)

tmpUsersDF = usersDF.join(userDetailDF, usersDF.id == userDetailDF.user_id, "inner").drop(usersDF.id).drop(userDetailDF.user_id)
# tmpUsersDF.show(5)

tmpOrdersDF = ordersDF.join(orderDetailDF, ordersDF.id == orderDetailDF.order_id, "inner").drop(ordersDF.id)
tmpOrdersDF = tmpOrdersDF.withColumnRenamed("created_at","order_created_at")
tmpOrdersDF = tmpOrdersDF.select("*").filter(tmpOrdersDF.order_created_at == datetime.date(datetime.today()))
# tmpOrdersDF.show(5)

orderByProductDF = tmpProductsDF.join(tmpOrdersDF, tmpProductsDF.id == tmpOrdersDF.product_id, "inner").drop(tmpProductsDF.id)
# orderByProductDF.show(5)

orderByUserDF = tmpUsersDF.join(tmpOrdersDF, tmpUsersDF.id == tmpOrdersDF.user_id, "inner").drop(tmpUsersDF.id)
# orderByUserDF.show(5)

#############################################################################################################################################

orderByProductDF = tmpProductsDF.join(tmpOrdersDF, tmpProductsDF.id == tmpOrdersDF.product_id, "inner").drop(tmpProductsDF.id)

orderByProductDF = orderByProductDF.groupBy("product_id", "Make", "Model", "Category", "inv_quantity")\
                .agg(sum("quantity").alias("Sold"),\
                    sum("total").alias("Revenue"))

orderByProductDF = orderByProductDF.withColumn("LeftOver", col("inv_quantity") - col("Sold")) \
        .withColumn("created_at", lit(datetime.date(datetime.today()))) \
	.select("product_id", "Make", "Model", "Category", "Sold", "Revenue", "LeftOver", "created_at")

##############################################################################################################################################

orderByUserDF = tmpUsersDF.join(tmpOrdersDF, tmpUsersDF.id == tmpOrdersDF.user_id, "inner").drop(tmpUsersDF.id)

orderByUserDF = orderByUserDF.withColumn("fullName", concat(col("firstname"), lit(" "), col("lastname")))
orderByUserDF = orderByUserDF.groupBy("user_id", "fullName", "country") \
                               .agg(sum("quantity").alias("Purchased"),\
                                    sum("total").alias("Revenue"))

orderByUserDF = orderByUserDF.withColumn("created_at", lit(datetime.date(datetime.today()))) \
                            .select("user_id", "fullName", "country", "Purchased", "Revenue", "created_at")


###############################################################################################################################################


orderByProductDF.write \
        .format("jdbc")\
        .option("driver","com.mysql.cj.jdbc.Driver")\
        .option("url", "jdbc:mysql://mysql_des:3306/myCompany")\
        .option("dbtable", "orderByProductView")\
        .option("user", "root")\
        .option("password", "debezium")\
        .mode("append")\
        .save()
records1 = orderByProductDF.count()
logger.info(f"Save to table orderByProductView: ({records1} records)")

orderByUserDF.write \
        .format("jdbc")\
        .option("driver","com.mysql.cj.jdbc.Driver")\
        .option("url", "jdbc:mysql://mysql_des:3306/myCompany")\
        .option("dbtable", "orderByUserView")\
        .option("user", "root")\
        .option("password", "debezium")\
        .mode("append")\
        .save()
records2 = orderByUserDF.count()
logger.info(f"Save to table orderByUserView: ({records2} records)")