import sys
from pyspark import SparkContext as sc

def extractCuisine(partId, list_of_records):
    if partId==0: 
        list_of_records.next() # skipping the first line
    import csv
    reader = csv.reader(list_of_records)
    for row in reader:
        cuisine = row[7]
        yield(cuisine)

def main(sc):
	NYC_REST = '/data/share/bdm/nyc_restaurants.csv'
	rest = sc.textFile(NYC_REST, use_unicode=False).cache()
	rest.take(3)
	list(enumerate(rest.first().split(',')))
	allCuisines = rest.mapPartitionsWithIndex(extractCuisine)
	allCuisines.take(10)

	file = open("output.txt", "w")
	for k in allCuisines.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y).collect():
    file.write(str(k) + "\n")
	file.close()

if __name__ == '__main__':
	sc = SparkContext()
	main(sc)