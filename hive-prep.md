## Create two DBs

CREATE DATABASE IF NOT EXISTS retail_ods;

CREATE DATABASE retail_edw;

### Hive string does not have precision like regular RDBMS
### External tables will only have metadata. managed tables have files and metadata
### When dropped external table files are still retained but for managed tables files will be deleted along with the metadata in the metastore


````
CREATE TABLE categories
(
  category_id int,
  category_department_id int,
  category_name string
  )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE TABLE customers
(
  customer_id int,
  customer_fname string,
  customer_lname string,
  customer_email string,
  customer_password string,
  customer_street string,
  customer_city string,
  customer_state string,
  customer_zipcode string
  )

  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

  CREATE TABLE departments
  (
    department_id int,
    department_name string
    )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

CREATE TABLE orders
(
  order_id int,
  order_date string,
  order_customer_id int,
  order_status string
)
PARTITIONED BY (order_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

CREATE TABLE order_items
(
order_item_id int,
order_item_order_id int,
order_item_order_date string,
order_item_product_id int,
order_item_quantity smallint,
order_item_subtotal float,
order_item_product_price float

  )
  PARTITIONED BY (order_month string)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

  CREATE TABLE products
  (
  product_id int,
  product_category_id int,
  product_name string,
  product_description string,
  product_price float,
  product_image string    
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
    STORED AS TEXTFILE;


  ````

  ## Create tables with hash partitioning

  ````
  CREATE TABLE orders_hash
  (
    order_id int,
    order_date string,
    order_customer_id int,
    order_status string
  )
  CLUSTERED BY (order_id) into 16 buckets
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE;

  CREATE TABLE order_items_hash
  (
  order_item_id int,
  order_item_order_id int,
  order_item_order_date string,
  order_item_product_id int,
  order_item_quantity smallint,
  order_item_subtotal float,
  order_item_product_price float

    )
    CLUSTERED BY (order_item_order_id) into 16 buckets
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
    STORED AS TEXTFILE;

  ````

Import all tables to Avro

  ````
  sqoop import-all-tables -m 1 --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" --username=root --password=cloudera --as-avrodatafile --warehouse-dir=/user/hive/warehouse/retail_db.db

  ````

  Move the avsc files to


  ````
  hadoop fs -mkdir /user/cloudera/sqoop_import/avsc
  hadoop fs -put  *.avsc  /user/cloudera/sqoop_import/avsc

  CREATE EXTERNAL TABLE categories
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
    LOCATION 'hdfs:///user/hive/warehouse/retail_db.db/categories'
    TBLPROPERTIES('avro.schema.url'='hdfs://quickstart.cloudera/user/cloudera/sqoop_import/avsc/categories.avsc');

  ````


  ## Create  table using files from sqoop import and LOAD DATA

  ````
  sqoop import -m 1 --connect "jdbc:mysql://quickstart.cloudera:3306/yelp_db" --username=root --password=cloudera --table business --fields-terminated-by '~' --target-dir  /user/cloudera/sqoop_import/yelp

  CREATE TABLE business
(
  id string,
  name string,
  neighborhood string,
  address string,
  city string,
  state string,
  postal_code string,
  latitude float,
  longitude float,
  stars float,
  review_count int,
  is_open smallint
  )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '~'
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/cloudera/sqoop_import/yelp' INTO TABLE business;

# After LOAD DATA the file will be moved from the source location
  ````

  ## Repeat same step as EXTERNAL table

  ````
  sqoop import -m 1 --connect "jdbc:mysql://quickstart.cloudera:3306/yelp_db" --username=root --password=cloudera --table business --fields-terminated-by '~' --target-dir  /user/cloudera/sqoop_import/yelp

  CREATE EXTERNAL TABLE business
  (
  id string,
  name string,
  neighborhood string,
  address string,
  city string,
  state string,
  postal_code string,
  latitude float,
  longitude float,
  stars float,
  review_count int,
  is_open smallint
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '~'
  STORED AS TEXTFILE;

  LOAD DATA INPATH '/user/cloudera/sqoop_import/yelp' INTO TABLE business;

  #Load Data does not use mapreduce. It just copies the file to hive warehouse

  ````
