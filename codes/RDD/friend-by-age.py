import pyspark as spark
from pyspark import SparkConf, SparkContext
# Create a SparkConf object with appropriate configuration
conf = SparkConf().setAppName("FriendsByAge").setMaster("local[*]")  # Use "local[*]" for local mode

# Initialize SparkContext
sc = SparkContext(conf=conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

try:
    # Read the data from the file
    lines = sc.textFile("datasets/fakefriends.csv")

    # Transform the data
    rdd = lines.map(parseLine)
    totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

    # Collect and print the results
    results = averagesByAge.collect()
    for result in results:
        print(result)
except Exception as e:
    print("An error occurred:", str(e))
finally:
    # Stop the SparkContext when done
    sc.stop()
