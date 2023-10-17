from pyspark import SparkConf, SparkContext

def parseLine(line):
    fields = line.split(',')
    first = fields[0]
    third = fields[2]
    return (int(first), float(third))


conf = SparkConf().setMaster("local").setAppName("Customer-Orders")
sc = SparkContext(conf = conf)
input = sc.textFile("datasets/customer-orders.csv")

parsedLines = input.map(parseLine)
stationTemps = parsedLines.reduceByKey(lambda x, y: x + y)
sorted_stationTemps = stationTemps.sortByKey()
results = sorted_stationTemps.collect()
for result in results:
    print(result[0], round(result[1],2))