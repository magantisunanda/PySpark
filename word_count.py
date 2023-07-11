import os
import sys
from pyspark import SparkContext
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

#spark = SparkSession.builder.appName("basic transformations").master("local[*]").getOrCreate()
sc = SparkContext("local[*]", "wordcount")
input = sc.textFile(r"C:\Users\kbpav\Desktop\wordcount.txt")

words = input.flatMap(lambda x: x.split(" "))

word_counts= words.map(lambda x: (x, 1))
final_count = word_counts.reduceByKey(lambda x, y: x + y)
result = final_count.collect()
for a in result:
  print(a)