### Import all tables to hive metastore problem6

````
sqoop import-all-tables \
--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username=root \
--password=cloudera \
--warehouse-dir /user/hive/warehouse/problem6.db \
--hive-import \
--hive-database problem6 \
--create-hive-table \
--as-textfile
````

### By Department list the products by descending rank of price

````
select d.department_id,p.product_id, product_price,
rank() over(partition by d.department_id order by p.product_price) as product_price_rank,
dense_rank() over(partition by d.department_id order by p.product_price) as product_dense_rank,p.product_name
from products p
inner join categories c on c.category_id = p.product_category_id
inner join departments d on c.category_department_id = d.department_id
order by d.department_id,product_price_rank desc,product_dense_rank
````

### Top 10 customers with highest number of products
````
select c.customer_id,c.customer_fname,count(distinct(oi.order_item_product_id))  as distinct_proucts
from customers c
inner join orders o on o.order_customer_id = c.customer_id
inner join order_items oi on o.order_id = oi.order_item_order_id
group by c.customer_id,c.customer_fname
order by distinct_proucts desc,c.customer_id limit 10;
````

### Find those products which are less than 100
````
select p.*
from products p
inner join order_items oi on oi.order_item_product_id = p.product_id
inner join orders o on o.order_id = oi.order_item_order_id
inner join customers c on c.customer_id = o.order_customer_id
where c.customer_id in (
select a.customer_id from
(select c.customer_id,c.customer_fname,count(distinct(oi.order_item_product_id))  as distinct_proucts
from customers c
inner join orders o on o.order_customer_id = c.customer_id
inner join order_items oi on o.order_id = oi.order_item_order_id
group by c.customer_id,c.customer_fname
order by distinct_proucts desc,c.customer_id limit 10) a)
and p.product_price < 100
````
