## Import all tables in to retail_stage.db as avro

````
sqoop import-all-tables \
-m 1 \
--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username=root \
--password=cloudera \
--as-avrodatafile \
--warehouse-dir /user/hive/warehouse/retail_stage.db \
--compress \
--compression-codec snappy

````

### Create avsc file and store schema file on hdfs
````
hadoop fs -get /user/hive/warehouse/retail_stage.db/orders/part-m-00000.avro
avro-tools getschema part-m-00000.avro > orders.avsc
hadoop fs -mkdir -p /user/hive/schemas/orders
hadoop fs -put orders.avsc /user/hive/schemas/orders

# Create an external table with avsc schema

create external table orders_sqoop
    STORED as AVRO
    LOCATION '/user/hive/warehouse/retail_stage.db/orders'
    TBLPROPERTIES('avro.schema.url'='/user/hive/schemas/orders/orders.avsc');
````

### Find the orders of day where maximum orders are received


````
# Date with maximum orders

select order_date,count(1) total_orders from orders_sqoop group by order_date order by total_orders desc limit 1

# Use this query to get the order_date to use in the where clause
select a.order_date from (select order_date,count(1) total_orders from orders_sqoop group by order_date order by total_orders desc limit 1) a

# Finally get all the orders on the date

select * from orders_sqoop y where y.order_date in (select a.order_date from (select order_date,count(1) total_orders from orders_sqoop group by order_date order by total_orders desc limit 1) a);

````
