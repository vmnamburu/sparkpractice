## Sorting

````
productRDD = sc.textFile("/user/cloudera/sqoop_import/products")
productsMap = productRDD.map(lambda x: (x.split(",")[4],x)).sortByKey()
````

Sort by Descending order

````
productsMap = productRDD.map(lambda x: (x.split(",")[4],x)).sortByKey(False)
````

````
productCatMap = productRDD.map(lambda x: (x.split(",")[1],x)).groupByKey().map(lambda x: (x[0],list(x[1])))

productCatMap = productRDD.map(lambda x: (x.split(",")[1],x)).groupByKey().map(lambda x: (x[0],list(x[1])))

Sort with in the list

productCatGroup = productCatMap.flatMap(lambda x: sorted(x[1],key = lambda k: k.split(",")[4]))

productCatGroup = productCatMap.flatMap(lambda x: sorted(x[1],key = lambda k: k.split(",")[4], reverse=True))
````
