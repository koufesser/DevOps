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
    bible_path = SparkFiles.get("Bible.txt") 
    all_words = []
    with open(bible_path, 'r', encoding='utf-8') as f:
        for line in f:
            words = line.lower().split()
            for word in words:
                all_words.append((word, 1))  # Создаем кортеж (word, 1)

    word_rdd = sc.parallelize(all_words)

    counts = word_rdd.reduceByKey(lambda a, b: a + b)
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
