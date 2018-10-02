### Copy orders to different formats

````
sqoop import -m 1 --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username=root --password=cloudera \
--table orders \
--fields-terminated-by '\t' \
--target-dir /user/cloudera/problem5/text

sqoop import -m 1 --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username=root --password=cloudera \
--table orders \
--fields-terminated-by '\t' \
--as-avrodatafile \
--target-dir /user/cloudera/problem5/avro

sqoop import -m 1 --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username=root --password=cloudera \
--table orders \
--fields-terminated-by '\t' \
--as-parquetfile \
--target-dir /user/cloudera/problem5/parquet
````

### Read Avro file through spark

````

from pyspark.sql import SQLContext

sqlCtx = SQLContext(sc)

dataFile = sqlCtx.read.format('com.databricks.spark.avro').load('/user/cloudera/problem5/avro')
# Write as avro file from the DataFrame
dataFile.write.format('com.databricks.spark.avro').save('/user/cloudera/problem5/avrooutput')

# Save as textfile gzip

dataFile.map(lambda x : (str(x[0])+'\t'+str(x[1])+'\t'+str(x[2])+'\t'+x[3])).saveAsTextFile('/user/cloudera/problem5/text-gzip-codec','org.apache.hadoop.io.compress.GzipCodec')

sqlCtx.setConf('spark.sql.parquet.compression.codec','snappy')

sqlContext.setConf("spark.sql.parquet.compression.codec","snappy");

dataFile.write.parquet('/user/cloudera/problem5/result-parquet')

````

### Read a parquet file
### The file was compressed

````
from pyspark.sql import SQLContext
sqlCtx = SQLContext(sc)
parqFileRDD = sqlCtx.read.parquet('/user/cloudera/problem5/result-parquet')

````


### Write this data as uncompresses
### Set Conf to snappy uncompressed
````

sqlCtx.setConf('spark.sql.parquet.compression.codec','uncompressed')
parqFileRDD.write.parquet('/user/cloudera/problem5/result-parquet-uncompress')

````

###Write as avro snappy

````
sqlCtx.setConf('spark.sql.avro.compression.codec','snappy')
parqFileRDD.write.avro('/user/cloudera/problem5/avro-snappy')
````

### Read avro snappy and write to JSON

````
avroData = sqlCtx.read.format('com.databricks.spark.avro').load('/user/cloudera/problem5/avro-snappy')
avroData.toJSON().saveAsTextFile('/user/cloudera/problem5/json-no-compress')
avroData.toJSON().saveAsTextFile('/user/cloudera/problem5/json-compress-gzip','org.apache.hadoop.io.compress.GzipCodec')
````

### Read Json and write as text file
````
jsonRDD = sqlCtx.read.json('/user/cloudera/problem5/json-no-compress')
jsonRDD.map(lambda x: str(x[0])+','+str(x[1])+','+str(x[2])+','+x[3]).saveAsTextFile('/user/cloudera/problem5/json-to-text')

````

### Read an avor file and write as a sequence file

````
dataFile = sqlCtx.read.format('com.databricks.spark.avro').load('/user/cloudera/problem5/avro')
# Sequence File can be writtern only for (key,Value)
dataFile.map(lambda x: (str(x[0]),str(x[0])+','+str(x[1])+','+str(x[2])+','+str(x[3]))).saveAsSequenceFile('/user/cloudera/problem5/output-sequence')
````

### Read a json file and save as csv

````
jsonRDD = sqlCtx.read.json('/user/cloudera/problem5/json-no-compress')

jsonRDD.map(lambda x: (str(x[0])+','+str(x[1])+','+str(x[2])+','+str(x[3]))).saveAsTextFile('/user/cloudera/problem5/json-2-text','org.apache.hadoop.io.compress.GzipCodec')

````

### Read a sequence file

````
seqData = sc.sequenceFile('/user/cloudera/problem5/output-sequence',keyClass='org.apache.hadoop.io.Text',valueClass='org.apache.hadoop.io.Text')
seqData.map(lambda x : x[1].split(',')).map(lambda x : (x[0],x[1],x[2],x[3]))
````
