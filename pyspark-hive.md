### Import all the tables to Hive

```
sqoop import-all-tables \
 --num-mappers 4 \
 --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
 --username=retail_dba \
 --password=cloudera \
 --hive-import \
 --hive-overwrite \
 --create-hive-table
```

sqoop import \
  --num-mappers 1 \
  --connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
  --username=retail_dba \
  --password=cloudera \
  --table products \
  --target-dir /user/cloudera/sqoop_import/products
