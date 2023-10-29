from pyspark import SparkContext, SparkConf
import time
import math
# Initialize Spark
conf = SparkConf().setAppName("PrimeNumbers")
sc = SparkContext(conf=conf)

# List of file paths
file_paths = ["data/uniform.txt", "data/gaussian.txt", "data/bernoulli.txt"]

def is_prime(n):
    if n <= 1:
        return False
    for i in range(2, int(math.sqrt(n)) + 1):
        if n % i == 0:
            return False
    return True
def find_primes(file_path):
    data = sc.textFile(file_path)
    start_time = time.time()  # Record start time
    prime_numbers = data.filter(lambda num: is_prime(int(num)))
    end_time = time.time()  # Record end time
    execution_time = end_time - start_time
    return prime_numbers, execution_time

# Process each file and find prime numbers
for file_path in file_paths:
    primes, execution_time = find_primes(file_path)
    print(f"Prime numbers in {file_path}:")
    print(primes.collect())
    print(f"Execution time for {file_path}: {execution_time:.2f} seconds")

# Stop Spark
sc.stop()
