NYC_REST = 'nyc_restaurants.csv'
rest = sc.textFile(NYC_REST, use_unicode=False).cache()
rest.take(3)
list(enumerate(rest.first().split(',')))

def extractCuisine(partId, list_of_records):
    if partId==0: 
        list_of_records.next() # skipping the first line
    import csv
    reader = csv.reader(list_of_records)
    for row in reader:
        cuisine = row[7]
        yield(cuisine)
        

allCuisines = rest.mapPartitionsWithIndex(extractCuisine)
allCuisines.take(10)

allCuisines.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).collect()