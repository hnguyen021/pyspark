# from pyspark import SparkContext
# sc = SparkContext('local', 'PySparkIntro')
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName('SparkIntro1').getOrCreate()  
# data = [1,2,3,4,5]
# rdd = sc.parallelize(data)
# squared_rdd = rdd.map(lambda x : x**2)
# even_rdd = rdd.filter(lambda x : x %2 == 0)
# collected_data = squared_rdd.collect()
# print(collected_data)

import pandas as pd

# Specify the path to your CSV file
csv_file = "datasets/1800.csv"

# Read the CSV file into a pandas DataFrame
df = pd.read_csv(csv_file)

# minTemps = df[df.apply(lambda rows: rows[df.columns[2]] == "TMIN", axis = 1)]
minTemps = df[df.apply(lambda x: x[2] == "TMIN", axis=1)]

#print(minTemps)
from pyspark import SparkContext

# Khởi tạo SparkContext
sc = SparkContext("local", "reduceByKey_example")

# Tạo một RDD với các cặp key-value
data = [("A", 1), ("B", 2), ("A", 3), ("B", 4), ("C", 5)]
rdd = sc.parallelize(data)

# Sử dụng reduceByKey để tính tổng các giá trị theo key
result = rdd.reduceByKey(lambda x, y: x + y)

# Thu thập kết quả và in ra màn hình
output = result.collect()
for (key, value) in output:
    print(f"Key: {key}, Sum: {value}")

# Đóng SparkContext
sc.stop()


