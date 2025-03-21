from pyspark import SparkConf, SparkContext, SparkFiles
from pyspark.sql import SparkSession
import os

conf = SparkConf().setAppName("Custom spark").setMaster("spark://spark-master:7077")
sc = SparkContext(conf=conf)
# Start the SparkSession
spark = SparkSession.builder \
                    .config(conf=conf) \
                    .getOrCreate()

try:
    print("0")
    text_file = sc.textFile(SparkFiles.get("Bible.txt"))
    print('3')
    words = text_file.flatMap(lambda line: line.lower().split())
    print('4')
    word_counts = words.map(lambda word: (word, 1))
    print('5')
    counts = word_counts.reduceByKey(lambda a, b: a + b)
    print('6')
    output = counts.collect()
    for (word, count) in output:
        print(f"{word}: {count}")

except Exception as e:
    print(f"Error occurred: {e}")

spark.stop()

# Import the necessary modules
# from pyspark import SparkConf, SparkContext
# from pyspark.sql import SparkSession

# conf = SparkConf().setAppName("My PySpark App").setMaster("spark://spark-master:7077")
# sc = SparkContext(conf=conf)
# # Start the SparkSession
# spark = SparkSession.builder \
#                     .config(conf=conf) \
#                     .getOrCreate()

# rdd = sc.parallelize(range(1, 100))

# print("THE SUM IS HERE: ", rdd.sum())
# # Stop the SparkSession
# spark.stop()
