## Get the highest product price

1. Create a tuple (product Id, product row)
2. Filter product ID 685 which has a comma in one of the fields
3. Get the price from the product row
4. Apply Reduce

````
productsRDD = sc.textFile("/user/cloudera/sqoop_import/products")
productsMap = productsRDD.map(lambda rec : (rec.split(",")[0],rec.split(","))).filter( lambda rec: rec[0] != '685').map(lambda rec: float(rec[1][4]))
productsMap.reduce( lambda x,y : x if x > y else y)
````

## When to Use Combiner logic and reducer logic

If the operation is additive both Combiner and Reducer will be the same. Ex. Addition

If the operation is not additive both combiner and reducer will be different. Ex. Average
