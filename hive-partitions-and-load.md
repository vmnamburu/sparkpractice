##get files from mysql for orders by status

## Static Partitioning

### Load files with the columns for each status
1. order_id
2. order_date
3. order_customer_id

````
select order_id,date_format(order_date,'%Y-%m-%d') as order_date,order_customer_id INTO OUTFILE '/tmp/orders_CLOSED.csv' FIELDS TERMINATED BY ',' from orders where order_status = 'CLOSED';
select order_id,date_format(order_date,'%Y-%m-%d') as order_date,order_customer_id INTO OUTFILE '/tmp/orders_PENDING_PAYMENT.csv' FIELDS TERMINATED BY ',' from orders where order_status = 'PENDING_PAYMENT';
select order_id,date_format(order_date,'%Y-%m-%d') as order_date,order_customer_id INTO OUTFILE '/tmp/orders_COMPLETE.csv' FIELDS TERMINATED BY ',' from orders where order_status = 'COMPLETE';
select order_id,date_format(order_date,'%Y-%m-%d') as order_date,order_customer_id INTO OUTFILE '/tmp/orders_PROCESSING.csv' FIELDS TERMINATED BY ',' from orders where order_status = 'PROCESSING';
select order_id,date_format(order_date,'%Y-%m-%d') as order_date,order_customer_id INTO OUTFILE '/tmp/orders_PAYMENT_REVIEW.csv' FIELDS TERMINATED BY ',' from orders where order_status = 'PAYMENT_REVIEW';
select order_id,date_format(order_date,'%Y-%m-%d') as order_date,order_customer_id INTO OUTFILE '/tmp/orders_PENDING.csv' FIELDS TERMINATED BY ',' from orders where order_status = 'PENDING';
select order_id,date_format(order_date,'%Y-%m-%d') as order_date,order_customer_id INTO OUTFILE '/tmp/orders_ON_HOLD.csv' FIELDS TERMINATED BY ',' from orders where order_status = 'ON_HOLD';
select order_id,date_format(order_date,'%Y-%m-%d') as order_date,order_customer_id INTO OUTFILE '/tmp/orders_CANCELED.csv' FIELDS TERMINATED BY ',' from orders where order_status = 'CANCELED';
select order_id,date_format(order_date,'%Y-%m-%d') as order_date,order_customer_id INTO OUTFILE '/tmp/orders_SUSPECTED_FRAUD.csv' FIELDS TERMINATED BY ',' from orders where order_status = 'SUSPECTED_FRAUD';
````
### Create Hive Managed Table with partition for Status

create table order_part_status
( order_id int,
  order_date date,
  order_customer_id int
  )
  partitioned by (order_status string)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


### load files by each partition

````
LOAD DATA LOCAL INPATH '/tmp/orders_CLOSED.csv' INTO TABLE order_part_status PARTITION(order_status= 'CLOSED');
LOAD DATA LOCAL INPATH  '/tmp/orders_PENDING_PAYMENT.csv' INTO TABLE order_part_status PARTITION(order_status= 'PENDING_PAYMENT');
LOAD DATA LOCAL INPATH  '/tmp/orders_COMPLETE.csv' INTO TABLE order_part_status PARTITION(order_status= 'COMPLETE');
LOAD DATA LOCAL INPATH  '/tmp/orders_PROCESSING.csv' INTO TABLE order_part_status PARTITION(order_status= 'PROCESSING');
LOAD DATA LOCAL INPATH  '/tmp/orders_PAYMENT_REVIEW.csv' INTO TABLE order_part_status PARTITION(order_status= 'PAYMENT_REVIEW');
LOAD DATA LOCAL INPATH  '/tmp/orders_PENDING.csv' INTO TABLE order_part_status PARTITION(order_status= 'PENDING');
LOAD DATA LOCAL INPATH  '/tmp/orders_ON_HOLD.csv' INTO TABLE order_part_status PARTITION(order_status= 'ON_HOLD');
LOAD DATA LOCAL INPATH  '/tmp/orders_CANCELED.csv' INTO TABLE order_part_status PARTITION(order_status= 'CANCELED');
LOAD DATA LOCAL INPATH  '/tmp/orders_SUSPECTED_FRAUD.csv' INTO TABLE order_part_status PARTITION(order_status= 'SUSPECTED_FRAUD');
````

###  Create Externally managed table partitioned by order_status

````
create external table order_ext_status
(
order_id int,
  order_date date,
  order_customer_id int
)
partitioned by (order_status string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/cloudera/exam_prep/hive_tables/orders'
````


### Create directory for each status to hold the parition

````
hadoop fs -mkdir  /user/cloudera/exam_prep/hive_tables/orders/CLOSED
hadoop fs -mkdir  /user/cloudera/exam_prep/hive_tables/orders/PENDING_PAYMENT
hadoop fs -mkdir  /user/cloudera/exam_prep/hive_tables/orders/COMPLETE
hadoop fs -mkdir  /user/cloudera/exam_prep/hive_tables/orders/PROCESSING
hadoop fs -mkdir  /user/cloudera/exam_prep/hive_tables/orders/PAYMENT_REVIEW
hadoop fs -mkdir  /user/cloudera/exam_prep/hive_tables/orders/PENDING
hadoop fs -mkdir  /user/cloudera/exam_prep/hive_tables/orders/ON_HOLD
hadoop fs -mkdir  /user/cloudera/exam_prep/hive_tables/orders/CANCELED
hadoop fs -mkdir  /user/cloudera/exam_prep/hive_tables/orders/SUSPECTED_FRAUD
````

## Move the files to the locations


````
hadoop fs -put /tmp/orders_CLOSED.csv  /user/cloudera/exam_prep/hive_tables/orders/CLOSED
hadoop fs -put /tmp/orders_PENDING_PAYMENT.csv  /user/cloudera/exam_prep/hive_tables/orders/PENDING_PAYMENT
hadoop fs -put /tmp/orders_COMPLETE.csv  /user/cloudera/exam_prep/hive_tables/orders/COMPLETE
hadoop fs -put /tmp/orders_PROCESSING.csv  /user/cloudera/exam_prep/hive_tables/orders/PROCESSING
hadoop fs -put /tmp/orders_PAYMENT_REVIEW.csv  /user/cloudera/exam_prep/hive_tables/orders/PAYMENT_REVIEW
hadoop fs -put /tmp/orders_PENDING.csv  /user/cloudera/exam_prep/hive_tables/orders/PENDING
hadoop fs -put /tmp/orders_ON_HOLD.csv  /user/cloudera/exam_prep/hive_tables/orders/ON_HOLD
hadoop fs -put /tmp/orders_CANCELED.csv  /user/cloudera/exam_prep/hive_tables/orders/CANCELED
hadoop fs -put /tmp/orders_SUSPECTED_FRAUD.csv  /user/cloudera/exam_prep/hive_tables/orders/SUSPECTED_FRAUD
````

# Now explicitly add the partition by altering the table


````
ALTER TABLE order_ext_status ADD PARTITION (order_status='CLOSED') LOCATION '/user/cloudera/exam_prep/hive_tables/orders/CLOSED'
ALTER TABLE order_ext_status ADD PARTITION (order_status='PENDING_PAYMENT') LOCATION '/user/cloudera/exam_prep/hive_tables/orders/PENDING_PAYMENT';
ALTER TABLE order_ext_status ADD PARTITION (order_status='COMPLETE') LOCATION '/user/cloudera/exam_prep/hive_tables/orders/COMPLETE';
ALTER TABLE order_ext_status ADD PARTITION (order_status='PROCESSING') LOCATION '/user/cloudera/exam_prep/hive_tables/orders/PROCESSING';
ALTER TABLE order_ext_status ADD PARTITION (order_status='PAYMENT_REVIEW') LOCATION '/user/cloudera/exam_prep/hive_tables/orders/PAYMENT_REVIEW';
ALTER TABLE order_ext_status ADD PARTITION (order_status='PENDING') LOCATION '/user/cloudera/exam_prep/hive_tables/orders/PENDING';
ALTER TABLE order_ext_status ADD PARTITION (order_status='ON_HOLD') LOCATION '/user/cloudera/exam_prep/hive_tables/orders/ON_HOLD';
ALTER TABLE order_ext_status ADD PARTITION (order_status='CANCELED') LOCATION '/user/cloudera/exam_prep/hive_tables/orders/CANCELED';
ALTER TABLE order_ext_status ADD PARTITION (order_status='SUSPECTED_FRAUD') LOCATION '/user/cloudera/exam_prep/hive_tables/orders/SUSPECTED_FRAUD';
````

## Dynamic Partitioning by using a stage table

### Unload the complete table from mysql


````
select order_id,date_format(order_date,'%Y-%m-%d') as order_date,order_customer_id,order_status INTO OUTFILE '/tmp/orders_PROCESSED.csv' FIELDS TERMINATED BY ',' from orders where order_status in ('CLOSED','COMPLETE','PROCESSING','PAYMENT_REVIEW','PENDING','PENDING_PAYMENT');

select order_id,date_format(order_date,'%Y-%m-%d') as order_date,order_customer_id,order_status INTO OUTFILE '/tmp/orders_HELD.csv' FIELDS TERMINATED BY ',' from orders where order_status in ('ON_HOLD','CANCELED','SUSPECTED_FRAUD');
````

### Create a hive managed stage table

````
create table order_stg_status
(
  order_id int,
  order_date date,
  order_customer_id int,
  order_status string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
````

### Load data in to the stage table

````
LOAD DATA LOCAL INPATH '/tmp/orders_PROCESSED.csv' INTO TABLE order_stg_status;
LOAD DATA LOCAL INPATH '/tmp/orders_HELD.csv' INTO TABLE order_stg_status;
````


### Create a Hive Managed table partitioned by Order Status

````
create table order_dyn_part_status
( order_id int,
  order_date date,
  order_customer_id int
  )
  partitioned by (order_status string)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
````

### Load Data in to the target table from the stage table by
### providing the partition column

````
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE order_dyn_part_status partition(order_status)
select * from order_stg_status;
````
