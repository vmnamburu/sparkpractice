Spark CSV : https://github.com/databricks/spark-csv
Changing ColumnType in pyspark : https://stackoverflow.com/questions/46432789/how-to-change-pyspark-data-frame-column-data-type

### Word Count by using sortBy

````
fileRDD = sc.textFile("/user/cloudera/pyspark/word_count_example.text")
wordsRDD = fileRDD.flatMap(lambda x : x.split(" "))
 wcRDD = wordsRDD.map( lambda x : (x,1)).reduceByKey(lambda x,y : x+y).sortBy(lambda x : x[1],False).take(10)

````

## Write an RDD as an Avro using toDF function
## Read the avro file and apply a different schema instead of default schema

````
oiRDD = sc.textFile("/user/cloudera/sqoop_import/order_items").map(lambda x : x.split(",")).map(lambda x :(x[1],(int(x[3]),float(x[4])))).groupByKey()
oiRDDAgg = oiRDD.map(lambda x : (x[0],sum(map(operator.itemgetter(0),x[1])),sum(map(operator.itemgetter(1),x[1]))))

#Write the file as avro. No structure is defined here

oiRDD.toDF().write.format("com.databricks.spark.avro").save('/user/cloudera/exam_samples/rdd_to_df_avro')
````




1. Read the avro file in to a DF
2. DF is having default column names as " 1, 2 etc"

```
from_rdd_to_avro = sqlCtx.read.format('com.databricks.spark.avro').load('/user/cloudera/exam_samples/rdd_to_df_avro')

from_rdd_to_avro.printSchema()
root
 |-- _1: string (nullable = true)
 |-- _2: long (nullable = true)
 |-- _3: double (nullable = true)

````
3. Create an explicit schema with columnNames

````
from pyspark.sql.types import *

schem = StructType([StructField('orderID',StringType(),True),StructField('noOfProds',LongType(),True),StructField('orderValue',DoubleType(),True)])

````
4. Now load the file by applying this schema

````
from_rdd_to_avro = sqlCtx.read.format('com.databricks.spark.avro').load('/user/cloudera/exam_samples/rdd_to_df_avro',schema=schem)
from_rdd_to_avro.printSchema()
root
 |-- orderID: string (nullable = true)
 |-- noOfProds: long (nullable = true)
 |-- orderValue: double (nullable = true)
````

## Repeat the process with a parquet file

Convert the RDD to DF and write as parquet

````
oiRDDAgg.toDF().write.parquet('/user/cloudera/exam_samples/rdd_to_df_parquet')

from_rdd_to_parquet = sqlCtx.read.parquet('/user/cloudera/exam_samples/rdd_to_df_parquet')
from_rdd_to_parquet.printSchema()
root
 |-- _1: string (nullable = true)
 |-- _2: long (nullable = true)
 |-- _3: double (nullable = true)

````
Define Schema

````
schem = StructType([StructField('orderID',StringType(),True),StructField('noOfProds',LongType(),True),StructField('orderValue',DoubleType(),True)])
````

Collect from the DF and create a new DF with the new Schema
This is not a recommended approach as it involves an action to read the entire DF in to a collection

````
from_rdd_to_pq_w_schema = sqlCtx.createDataFrame(from_rdd_to_parquet.collect(),schem)
from_rdd_to_pq_w_schema.printSchema()
root
 |-- orderID: string (nullable = true)
 |-- noOfProds: long (nullable = true)
 |-- orderValue: double (nullable = true)

````


# DataFrame from a RDD with Row


````
productsRDD = sc.textFile("/user/cloudera/sqoop_import/products").map(lambda x : x.split(",")).map(lambda x : (int(x[0]),int(x[1]),x[2],x[3],float(x[4]),x[5]))

schema = StructType([  StructField('product_id',IntegerType(),True),  StructField('product_category_id',IntegerType(),True),  StructField('product_name',StringType(),True),StructField('product_description',StringType(),True),StructField('product_price',DoubleType(),True),  StructField('product_image',StringType(),True)])

productsDF = sqlCtx.createDataFrame(productsRDD,schema)
productsDF.printSchema()
root
 |-- product_id: integer (nullable = true)
 |-- product_category_id: integer (nullable = true)
 |-- product_name: string (nullable = true)
 |-- product_description: string (nullable = true)
 |-- product_price: double (nullable = true)
 |-- product_image: string (nullable = true)

````

````
productsAggDF = productsDF.filter(productsDF.product_price < 100).groupBy(productsDF.product_category_id).agg(F.max(productsDF.product_price).alias('max_price'),F.min(productsDF.product_price).alias('min_price'),F.countDistinct(productsDF.product_id).alias('distCount'),F.avg(productsDF.product_price).alias('avg_price'))
````

1. groupBy(<column> or <list of columns>)
2. agg(function(column).alias('column_name'))


````
productsDF.registerTempTable('products')
productsAgg = sqlCtx.sql('select product_category_id,max(product_price) as max_price,min(product_price) as min_price,count(product_id) as noProducts,avg(product_price) as avg_price  from products where product_price < 100 group by product_category_id');

````


## Some Datetime Processing

````
ordersRDD = sc.textFile('/user/cloudera/sqoop_import/orders').map(lambda x: x.split(",")).map(lambda x: Row(order_id=int(x[0]),order_date=datetime.strptime(x[1].split(" ")[0],'%Y-%m-%d'),order_customer_id=int(x[2]),order_status=x[3]))
ordersDF = sqlCtx.createDataFrame(ordersRDD)
sqlCtx.registerTempTable(ordersDF)

orderAgg = sqlCtx.sql('select order_customer_id,year(order_date) as order_year,month(order_date) as order_month, order_status, count(1) as ordCount from orders group by order_customer_id,year(order_date),month(order_date),order_status')

orderAgg = ordersDF.groupBy(ordersDF.order_customer_id,ordersDF.order_status,F.year(ordersDF.order_date).alias('order_year'),F.month(ordersDF.order_date).alias('order_month')).agg(F.count(ordersDF.order_id).alias('order_count'))

````
