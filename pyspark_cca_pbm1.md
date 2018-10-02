## Problem Scenario 1

### Export Orders as an Avro file with Snappy compression

````
sqoop import -m 1 --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username=root --password=cloudera --table orders --compress --compression-codec org.apache.hadoop.io.compress.SnappyCodec --target-dir /user/cloudera/problem1/orders --as-avrodatafile

````

### Export order Items as an Avro file with Snapp compression

````
sqoop import -m 1 --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username=root --password=cloudera --table order_items --compress --compression-codec org.apache.hadoop.io.compress.SnappyCodec --target-dir /user/cloudera/problem1/order-items --as-avrodatafile

````

### Load orders and order items as DataFrames

````
from pyspark.sql import SQLContext
from datetime import datetime
from pyspark.sql.types import DoubleType
sqlContext = SQLContext(sc)
dfOrders = sqlContext.read.format("com.databricks.spark.avro").load('/user/cloudera/problem1/orders')
dfOrderItems = sqlContext.read.format("com.databricks.spark.avro").load('/user/cloudera/problem1/order-items')

joinedOrderDF = dfOrders.join(dfOrderItems,dfOrders.order_id == dfOrderItems.order_item_order_id)

joinedOrderDF.groupBy([datetime.fromtimestamp(joinedOrderDF.order_date/1000),order_status]).sum(joinedOrderDF.order_item_subtotal).take(2)

joinedOrderDF.groupBy([joinedOrderDF.order_status,datetime.fromtimestamp(joinedOrderDF.order_date.cast("float")/float(1000))]).sum(joinedOrderDF.order_item_subtotal).take(2)

joinGroup = joinedOrderDF.groupBy([joinedOrderDF.order_status,datetime.fromtimestamp(joinedOrderDF.order_date.cast("float")/float(1000))]).sum('order_item_subtotal').withColumnRenamed('sum(order_item_subtotal)','order_total')


````

### Via Spark SQL

````
joinedOrderDF.registerTempTable("order_items_join")
sqlContext.sql("select to_date(from_unixtime())")

df = sqlContext.sql("select to_date(from_unixtime(cast(order_date/1000 as bigint))) as order_formatted_date, order_status, cast(sum(order_item_subtotal) as DECIMAL (10,2)) as total_amount, count(distinct(order_id)) as total_orders from order_items_join group by to_date(from_unixtime(cast(order_date/1000 as bigint))), order_status order by order_formatted_date desc,order_status,total_amount desc, total_orders");

````
