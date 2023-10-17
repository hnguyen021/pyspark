from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("datasets/fakefriends-header.csv")
    
print("Here is our inferred schema:")
people.printSchema()

friendByAge = people.select("age","friends")
friendByAge.groupBy("age").agg(func.round(func.avg("friends"),2).alias("avg-friends")).sort("age").show()

# print("Let's display the name column:")
# people.select("name").show()

# print("Filter out anyone over 21:")
# people.filter(people.age < 21).show()

# print("Group by age")
# people.groupBy("age").count().show()

# print("Make everyone 10 years older:")
# people.select(people.name, people.age + 10).show()

spark.stop()

