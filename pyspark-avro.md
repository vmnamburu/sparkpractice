##Avro

Launch pyspark with Avro jars from databricks

````
pyspark --packages com.databricks:spark-avro_2.11:4.0.0

````

Create a SQL Context and load the avro files


````
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
df = sqlContext.read.format("com.databricks.spark.avro").load('/user/hive/warehouse/retail_db.db/products')
df
DataFrame[product_id: int, product_category_id: int, product_name: string, product_description: string, product_price: float, product_image: string]

````
