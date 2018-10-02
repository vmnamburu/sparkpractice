# Find the Sum of Order Total by Day (Excluding CANCELED )

1. Read the orders File and create a tuple of (Order ID,Order Row)



````
ordersRDD = sc.textFile("/user/cloudera/sqoop_import/orders").map(lambda x : (x.split(",")[0],x))
````
2. Create a function to filter out CANCELED orders

````
def isOrderProcessed(order):
  if order[1].split(",")[3] == 'CANCELED':
    return False
  else:
    return True
````

3. Apply Filter function using isOrderProcessed

````
processedOrdersRDD = ordersRDD.filter(isOrderProcessed)
````

4. Read the order items file and create a tuple of (Order ID,Order  Item Row)

````
orderItemsRDD = sc.textFile("/user/cloudera/sqoop_import/order_items").map(lambda x : (x.split(",")[1],x))
````

5. Join orderItems to Orders. The common key OrderID becomes the join key

````
joinedOrdersRDD = orderItemsRDD.join(processedOrdersRDD)

````

6. Examine the result Rows

````
>>> for i in joinedOrdersRDD.take(5):
...     print i
...
(u'35540', (u'88767,35540,1004,1,399.98,399.98', u'35540,2014-02-28 00:00:00.0,5028,CLOSED'))
(u'35540', (u'88768,35540,797,2,35.98,17.99', u'35540,2014-02-28 00:00:00.0,5028,CLOSED'))
(u'35236', (u'87999,35236,365,1,59.99,59.99', u'35236,2014-02-27 00:00:00.0,3987,PENDING'))
(u'35236', (u'88000,35236,191,1,99.99,99.99', u'35236,2014-02-27 00:00:00.0,3987,PENDING'))
(u'35236', (u'88001,35236,365,1,59.99,59.99', u'35236,2014-02-27 00:00:00.0,3987,PENDING'))

````

7. Create a RDD that is (OrderDate,OrderSubTotal)

````
orderValueByDate = joinedOrdersRDD.map(lambda x: ( x[1][1].split(",")[1], float(x[1][0].split(",")[4])))
````
8. Apply Reduce By Key to calculate sum of order OrderSubTotal

````
orderValueByDate = orderValueByDate.reduceByKey(lambda x,y : x+y)

>>> for i in  orderValueByDate.take(5):
...     print i
...
(u'2013-09-19 00:00:00.0', 95749.76999999999)
(u'2013-11-29 00:00:00.0', 134621.74000000011)
(u'2013-12-06 00:00:00.0', 125361.91000000009)
(u'2014-06-17 00:00:00.0', 72956.209999999963)
(u'2013-11-07 00:00:00.0', 122777.55000000005)

````

9. Cross Verify in MySQL

````
mysql> select sum(order_item_subtotal) from order_items oi, orders o where o.order_id = oi.order_item_order_id and order_date='2013-09-19' and order_status !='CANCELED';
+--------------------------+
| sum(order_item_subtotal) |
+--------------------------+
|         95749.7717399597 |
+--------------------------+
1 row in set (0.10 sec)

````


# Average Order Revenue per Day

1. Get distinct (OrderDate,order_id)
2. Get a tuple (Orderdate,1) and apply reduceByKey
3. Get (OrderDate,order_item_subtotal) and apply reduceByKey
4. Join the two RDDs on ORder date

````
ordersRDD = sc.textFile("/user/cloudera/sqoop_import/orders").map(lambda x : (x.split(",")[0],x))
orderItemsRDD = sc.textFile("/user/cloudera/sqoop_import/order_items").map(lambda x : (x.split(",")[1],x))
joinedOrdersRDD = orderItemsRDD.join(ordersRDD)
ordersPerDay = joinedOrdersRDD.map(lambda x : (x[1][1].split(",")[1],x[1][1].split(",")[0])).distinct().map(lambda x: (x[0],1)).reduceByKey(lambda x,y: x+y)
orderRevenueByDate = joinedOrdersRDD.map(lambda x: ( x[1][1].split(",")[1], float(x[1][0].split(",")[4]))).reduceByKey(lambda x,y : x+y)
finalRDD = orderRevenueByDate.join(ordersPerDay).map(lambda x: (x[0],x[1][0]/x[1][1]))

mysql> select sum(order_item_subtotal), count(distinct order_item_order_id) from order_items oi, orders o where o.order_id = oi.order_item_order_id and order_date='2013-09-19';
+--------------------------+-------------------------------------+
| sum(order_item_subtotal) | count(distinct order_item_order_id) |
+--------------------------+-------------------------------------+
|         99324.3818283081 |                                 165 |
+--------------------------+-------------------------------------+


````
