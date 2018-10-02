### Export products to /user/cloudera/products as pipe DELIMITED

````

sqoop import -m 1 \
--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username=root \
--password=cloudera \
--table products \
--fields-terminated-by '|' \
--target-dir /user/cloudera/products

````

### Move files to /user/cloudera/problem2/products

````
hadoop fs -mkdir -p /user/cloudera/problem2/products
hadoop fs -mv /user/cloudera/products/* /user/cloudera/problem2/products

````

### Change permissions

````
$ hadoop fs -ls /user/cloudera/problem2/products
Found 2 items
-rw-r--r--   1 cloudera cloudera          0 2017-12-20 23:52 /user/cloudera/problem2/products/_SUCCESS
-rw-r--r--   1 cloudera cloudera     173993 2017-12-20 23:52 /user/cloudera/problem2/products/part-m-00000
$ hadoop fs -chmod 765 /user/cloudera/problem2/products/*
$ hadoop fs -ls /user/cloudera/problem2/products
Found 2 items
-rwxrw-r-x   1 cloudera cloudera          0 2017-12-20 23:52 /user/cloudera/problem2/products/_SUCCESS
-rwxrw-r-x   1 cloudera cloudera     173993 2017-12-20 23:52 /user/cloudera/problem2/products/part-m-00000

````

### Create DataFrame and find
1. Max Price
2. No of products
3. Average Price
4. Min Price

````

from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql import functions as F

sqlCtx = SQLContext(sc)

products = sc.textFile('/user/cloudera/problem2/products').map(lambda x : x.split('|')).map(lambda x : Row(product_id=int(x[0]),product_category_id=x[1],product_name=x[2],product_description=x[3],product_price=float(x[4]),product_image=x[5]))

productsDFF = sqlCtx.createDataFrame(products)

prodCatPrice = productsDFF.filter(productsDFF.product_price < 100).groupBy(productsDFF.product_category_id).agg(F.max(productsDFF.product_price).alias('max_price'),F.countDistinct(productsDFF.product_id).alias('prod_count'),F.avg(productsDFF.product_price).alias('avg_price'),F.min(productsDFF.product_price).alias('min_price'))

#Write to an avro file
prodCatPrice.write.format('com.databricks.spark.avro').save('/user/cloudera/problem2/products/result_df')


````

### Using SparkSQL

````
productsDFF.registerTempTable('products')
sqlResult = sqlCtx.sql('select product_category_id,max(product_price) as max_price,count(distinct product_id) as prod_count,avg(product_price) as avg_price,min(product_price) as min_price from products where product_price < 100 group by product_category_id order by product_category_id desc')

sqlResult.show()

sqlResult.write.format('com.databricks.spark.avro').save('/user/cloudera/problem2/results/result_sq')

````

### using RDD

#### aggregateByKey is complex with syntax and no of parameters
#### It is better to start with minimal parameters and expand

````
products = sc.textFile('/user/cloudera/problem2/products').map(lambda x: x.split('|')).map( lambda x: (int(x[1]),float(x[4]))).filter(lambda x : x[1] < 100)

#Apply Max
prodAggRDD1 =  products.aggregateByKey(0.0,lambda x,y : max(x,y),lambda x,y : max(x,y))
#Apply Max an Min (Zero Value, SeqOp, ComOp)
prodAggRDD2 =  products.aggregateByKey((0.0,99999.0),lambda x,y : (max(x[0],y),min(x[1],y)),lambda x,y : (max(x[0],y[0]),min(x[1],y[1])))
#Apply Max, Min, sum
prodAggRDD3 =  products.aggregateByKey((0.0,99999.0,0),lambda x,y : (max(x[0],y),min(x[1],y),x[2]+y),lambda x,y : (max(x[0],y[0]),min(x[1],y[1]),x[2]+y[2]))
#Apply Max, Min, sum,Count
prodAggRDD =  products.aggregateByKey((0.0,99999.0,0,0),lambda x,y : (max(x[0],y),min(x[1],y),x[2]+y,x[3]+1),lambda x,y : (max(x[0],y[0]),min(x[1],y[1]),x[2]+y[2],x[3]+y[3]))
# Calculate Average
prodAggRDD =  prodAggRDD.map(lambda x : (x[0],x[1][0],x[1][1],x[1][2]/x[1][3],x[1][3]))

prodAggRDD.toDF().write.format('com.databricks.spark.avro').save('/user/cloudera/problem2/results/result_rdd')
````
