pyspark - By default uses YARN
To run in local use pyspark --master local


To connect to JDBC run pyspark with classpath to the jdbc driver jar

````
pyspark --driver-class-path /usr/lib/hive/lib/mysql-connector-java.jar
````

Import the SQLContext and create a SQLContext from sc

````
from pyspark.sql import SQLContext

sqlCtx = SQLContext(sc)
````

Create a JDBC URL and load a table

```
jdbcurl = "jdbc:mysql://quickstart.cloudera:3306/retail_db?user=retail_dba&password=cloudera"
df = sqlCtx.load(source="jdbc",url=jdbcurl,dbtable="departments")

for rec in df.collect():
	print rec
````

## Run pyspark from spark-submit

````
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("pyspark")
sc = SparkConext(conf=conf)
dataRDD = sc.textFile("/user/cloudera/sqoop_import/order_items")
for line in dataRDD.take():
  print line
dataRDD.saveAsTextFile("/user/cloudera/sqoop_import/order_items_rdd_sv_as_txt")
````
### Change Log Level in log4j.properties

Edit log4j.properties in /etc/spark/conf for the Log Level property

````
log4j.rootCategory=WARN, console
````

## Create Sequence Files

````
from pyspark import SparkContext,SparkConf

conf = SparkConf().setAppName("Pyspark")
sc = SparkContext(conf=conf)
dataRDD = sc.textFile("/user/cloudera/pyspark/departments")

#Convert to Map
for tpl in dataRDD.map(lambda x:tuple(x.split(",",1))).collect():
    print tpl

dataRDD.map(lambda x:tuple(x.split(",",1))).saveAsSequenceFile("/user/cloudera/pyspark/deparments_seq")

````

## Read a Sequence File without mentioning the key values

````
>>> sequenceRDD = sc.sequenceFile("/user/cloudera/pyspark/departmentsSeq")
>>> for i in sequenceRDD.collect():
...     print i
...
(u'2', u'Fitness')
(u'3', u'Footwear')
(u'4', u'Apparel')
(u'5', u'Golf')
(u'6', u'Outdoors')
(u'7', u'fanshop')
(u'8000', u'Department 8000')
(u'9000', u'Department 9000')

````
## Read a Sequence File by mentioning the key values

Mention the types of Key Value Pairs


````
>>> sRDD = sc.sequenceFile("/user/cloudera/pyspark/departmentsSeq","org.apache.hadoop.io.IntWritable","org.apache.hadoop.io.Text")
>>> for i in sRDD.collect():
...     print i
...
(u'2', u'Fitness')
(u'3', u'Footwear')
(u'4', u'Apparel')
(u'5', u'Golf')
(u'6', u'Outdoors')
(u'7', u'fanshop')
(u'8000', u'Department 8000')
(u'9000', u'Department 9000')

````

## Create file using saveAsNewAPIHadoopFile

Parameters are

Path
1. Path
2. The Mapreduce File Format  (Can be Sequencefile etc)
3. Key class
4. Value class



````
>>> dataRDD = sc.textFile("/user/cloudera/pyspark/departments")
>>> path = "/user/cloudera/pyspark/hdfsoutput"
>>> filefmt = "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat"
>>> keycls = "org.apache.hadoop.io.Text"
>>> valuecls = "org.apache.hadoop.io.Text"
>>> dataRDD.map(lambda x : tuple(x.split(",",1))).saveAsNewAPIHadoopFile(path,filefmt,keyClass=keycls,valueClass=valuecls)

````
A sequence file is created in the path

````
[cloudera@quickstart ~]$ hadoop fs -ls /user/cloudera/pyspark/hdfsoutput
Found 3 items
-rw-r--r--   1 cloudera cloudera          0 2017-12-08 15:50 /user/cloudera/pyspark/hdfsoutput/_SUCCESS
-rw-r--r--   1 cloudera cloudera        185 2017-12-08 15:50 /user/cloudera/pyspark/hdfsoutput/part-r-00000
-rw-r--r--   1 cloudera cloudera        136 2017-12-08 15:50 /user/cloudera/pyspark/hdfsoutput/part-r-00001

````

## Reading from hive

````
from pyspark.sql import HiveContext

sqlContext = HiveContext(sc)
dataRDD = sqlContext.sql("select * from departments")
````


## Reading from JSON File

Create a JSON File and save in hdfs

````
hadoop fs -put departmetns.json /user/cloudera/pyspark/departments.json

````

Read the JSON using sqlContext

````
>>> from pyspark.sql import SQLContext
>>> sqlCtx = SQLContext(sc)
>>> departmentsJson = sqlCtx.jsonFile("/user/cloudera/pyspark/departments.json")
>>> departmentsJson.registerTempTable("djson")
>>> sqlCtx.sql("select * from djson")
DataFrame[deparment_id: bigint, department_name: string]
>>> for i in sqlCtx.sql("select * from djson").collect():
...     print i
...
Row(deparment_id=2, department_name=u'Fitness')
Row(deparment_id=3, department_name=u'Footwear')
Row(deparment_id=4, department_name=u'Apparel')
Row(deparment_id=5, department_name=u'Golf')
Row(deparment_id=6, department_name=u'Outdoors')
Row(deparment_id=7, department_name=u'fanshop')
Row(deparment_id=8000, department_name=u'Department 8000')
Row(deparment_id=9000, department_name=u'Department 9000')

````

Write data to HDFS as a JSON File

````
sqlCtx.sql("select * from djson").toJSON().saveAsTextFile("/user/cloudera/pyspark/deparmentsJSON")

[cloudera@quickstart cert_prep]$ hadoop fs -ls /user/cloudera/pyspark/deparmentsJSON
Found 3 items
-rw-r--r--   1 cloudera cloudera          0 2017-12-08 16:21 /user/cloudera/pyspark/deparmentsJSON/_SUCCESS
-rw-r--r--   1 cloudera cloudera        234 2017-12-08 16:21 /user/cloudera/pyspark/deparmentsJSON/part-00000
-rw-r--r--   1 cloudera cloudera        163 2017-12-08 16:21 /user/cloudera/pyspark/deparmentsJSON/part-00001

````

## Wordcount Example

````
dataRDD = sc.textFile("/user/cloudera/pyspark/word_count_example.text").flatMap(lambda x: x.split()).map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
````
Steps performed are
1. Split each line and create a flatMap
2. Create a (word,1) tuple
3. Apply ReduceByKey to calculate the count of each word

### output
````
>>> dataRDD.collect()
[(u'and', 1), (u'Which', 2), (u'is', 1), (u'reduce', 1), (u'file', 1), (u'answer', 1), (u'for', 2), (u'real-time:', 1), (u'Wordcount', 1), (u'only', 1), (u'4.', 1), (u'3', 1), (u'2,', 1), (u'you', 1), (u'was', 2), (u'A', 1), (u'C', 2), (u'This', 1), (u'test', 1), (u'job', 1), (u'key', 1), (u'The', 1), (u'implement', 1), (u'a', 4), (u'flatmap', 1), (u'this', 1), (u'will', 1), (u'following', 1), (u'the', 1), (u'Hadoop', 1), (u'one', 1), (u'How', 1), (u'in', 3), (u'times', 2), (u'day?', 2), (u'log', 1), (u'no', 1), (u'exactly', 1), (u'contains', 1), (u'by', 3), (u'to', 2), (u'4', 1), (u'Using', 1), (u'more', 1), (u'B', 1), (u'D', 1), (u'queries', 1), (u'user', 6), (u'entries', 1), (u'than', 1), (u'like', 1), (u'1,', 1), (u'3,', 1), (u'visited', 6), (u'page', 6)]

````


### Sort the Wordcount to get the top 5 words

````
dataRDD.map(lambda x : (x[1],x[0])).sortByKey(ascending=False).map(lambda x: (x[1],x[0])).take(5)
[(u'user', 6), (u'visited', 6), (u'page', 6), (u'a', 4), (u'in', 3)]

````

Steps performed are
1. Swap the tuple elements to (NN,word)
2. Apply SortByKey with Descending order
3. Apply map to reverse back to (word,NN)
