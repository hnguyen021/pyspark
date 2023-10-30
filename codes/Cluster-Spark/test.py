# from pyspark import SparkContext
# access_key_id = 'AKIA6BDM3G4WEWJQKGE3'
# secret_access_key = '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'

# # Initialize a SparkContext
# sc = SparkContext(appName="ReadFromS3RDD")

# # Set AWS access and secret keys if needed (use IAM Role or environment variables for security)
# sc._jsc.hadoopConfiguration().set("fs.s3.access.key", access_key_id)
# sc._jsc.hadoopConfiguration().set("fs.s3.secret.key", secret_access_key)
# # sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")




# # sc.addPyFile("resources/hadoop-aws-3.2.2.jar")
# # Read a file from S3 using textFile
# s3_url = "s3://pyspark-datasets/ml-1m/ratings.dat"
# rdd = sc.textFile(s3_url)
# ratings = rdd.map(lambda l: l.split("::")).map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))
# ratings = sc.parallelize(ratings)
# # print(type(ratings))
# # Perform RDD operations
# ratings.collect()  # Collect the data, this is just an example

# # Stop the SparkContext
# sc.stop()
from pyspark import SparkContext
import re
# Khởi tạo SparkContext
sc = SparkContext("local", "word_count_example")

# Danh sách các từ cần tìm
words_to_find = ["apple", "banana", "cherry", "date", "elderberry"]

# Tạo một broadcast variable để chia sẻ danh sách giữa các task
broadcast_words_to_find = sc.broadcast(words_to_find)

# Tạo một RDD chứa các đoạn văn bản
text_rdd = sc.parallelize([
    "I have an apple and a banana.",
    "Cherries are delicious.",
    "Do you like dates or elderberries?"
])
pattern = r"[.,/?]"
words_rdd = text_rdd.flatMap(lambda line: line.split(" "))
words_rdd = words_rdd.map(lambda word :re.sub(pattern, '', word))
# words_rdd = words_rdd.collect()
# print(words_rdd)
# Sử dụng broadcast variable để đếm số lần xuất hiện của từng từ trong các đoạn văn bản
word_counts = words_rdd.filter(lambda word: word in broadcast_words_to_find.value).map(lambda word : (word,1))
result = word_counts.reduceByKey(lambda x,y :x+y)
output = result.collect()
for (key, value) in output:
    print(f"Word: {key}, Count: {value}")

broadcast_words_to_find.unpersist()

sc.stop()
