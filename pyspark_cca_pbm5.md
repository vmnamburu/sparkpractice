### Create a table which is a replica of products
### Add Two columns and populate them

````
create table products_replica as select * from products;
alter table products_replica add primary key (product_id);
alter table products_replica add column (product_grade int, product_sentiment varchar(100));
````



````
sqoop import -m 3 --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username=root --password=cloudera \
--table products_replica \
--target-dir /user/cloudera/prbm5/products-text \
--fields-terminated-by '|' \
--lines-terminated-by '\n' \
--null-non-string -1 \
--null-string 'NOT-AVAILABLE' \
--where "product_id between 1 and 1000" \
--boundary-query 'select min(product_id),max(product_id) from products_replica where product_id between 1 and 1000' \
--outdir /home/cloudera/cert_prep \

sqoop import -m 2 --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username=root --password=cloudera \
--table products_replica \
--target-dir /user/cloudera/prbm5/products-text-part1 \
--fields-terminated-by '*' \
--lines-terminated-by '\n' \
--null-non-string -1000 \
--null-string 'NA' \
--where "product_id <= 1111" \
--boundary-query 'select min(product_id),max(product_id) from products_replica where product_id <= 1111' \
--outdir /home/cloudera/cert_prep

sqoop import -m 5 --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username=root --password=cloudera \
--table products_replica \
--target-dir /user/cloudera/prbm5/products-text-part2 \
--fields-terminated-by '*' \
--lines-terminated-by '\n' \
--null-non-string -1000 \
--null-string 'NA' \
--where "product_id > 1111" \
--boundary-query 'select min(product_id),max(product_id) from products_replica where product_id > 1111' \
--outdir /home/cloudera/cert_prep

````

### Merget Part1 and Part2
````
sqoop merge \
--class-name products_replica \
--jar-file /tmp/sqoop-cloudera/compile/8ef3656a7b3af1e58d89eb552a07316c/products_replica.jar \
--new-data /user/cloudera/prbm5/products-text-part2 \
--onto /user/cloudera/prbm5/products-text-part1 \
--target-dir /user/cloudera/prbm5/products-text-bothparts \
--merge-key product_id

````
### Create a Sqoop Job to import products_replica table incrementally
## Note space between -- and import
````
sqoop job --create first_sqoop_job \
-- import \
--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username=root \
--password=cloudera \
--table products_replica \
--target-dir /user/cloudera/pbm5/products-incremental \
--check-column product_id \
--incremental append \
--last-value 0
````


### Execute the job
````
sqoop job --exec first_sqoop_job
````

### Insert new products into prdoucts_replica
````
insert into products_replica values(1380,4,'something 2','something 2',40,'no image',2,'STRONG');
insert into products_replica values(1381,5,'something 2','something 2',40,'no image',2,'STRONG');
````

## Create Hive DB and Table
````
create database problem5;

create table products_hive
(
  product_id int,
  product_category_id int,
  product_name string,
  product_description string,
  product_price float,
  product_image string
  )
````

### Sqoop job to import products_replica to products_hive

````
sqoop job --create first_hive_job -- import \
--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username=root \
--password=cloudera \
--table products_replica \
--check-column product_id \
--incremental append \
--last-value 0 \
--hive-import \
--hive-database problem5 \
--hive-table products_hive
````
