## For an export, Specifying --jar-file and --class-name from a previous import does not need the delimiters to be mentioned explicity if different from default


### Create table in mysql to export from hive datafiles
````
mysql> create table products_external (product_id int(11) primary key,
    -> product_grade int(11),
    -> product_category_id int(11),
    -> product_name varchar(100),
    -> product_description varchar(100),
    -> product_price float,
    -> product_image varchar(500),
    -> product_sentiment varchar(100));

````

###
````

sqoop export \
--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username=root \
--password=cloudera \
--table products_external \
--export-dir /user/hive/warehouse/problem5.db/products_hive/ \
--input-fields-terminated-by '\001' \
--input-null-non-string "null" \
--input-null-string "null" \
--update-mode allowinsert \
--update-key product_id \
--columns "product_id,n,product_name,product_description,product_price,product_image,product_grade,product_sentiment"

````
