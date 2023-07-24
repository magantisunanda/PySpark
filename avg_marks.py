#read the input data in json format and find 1.Average Marks for each student ,2. Top 3 students having max marks in math,3. Pivot data into spark 

import os
import sys
from pyspark import SparkContext
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import max
from pyspark.sql.functions import col
from pyspark.sql import functions as F

conf = SparkConf().setAppName("Read JSON data")
sc = SparkContext.getOrCreate(conf)
spark = SparkSession(sc)

data = """
[
  {
    "name": "John",
    "age": 20,
    "subjects": [
      {
        "name": "Math",
        "score": 90
      },
      {
        "name": "Physics",
        "score": 85
      }
    ]
  },
  {
    "name": "Alice",
    "age": 22,
    "subjects": [
      {
        "name": "Math",
        "score": 95
      },
      {
        "name": "Physics",
        "score": 88
      },
      {
        "name": "Chemistry",
        "score": 92
      }
    ]
  }
]
"""

df = spark.read.json(sc.parallelize([data]))
#df.show(truncate=False)
df2=df.select(df["name"].alias("student"),df.age,explode(df.subjects).alias("subject"))
#df2.show()

#df.groupby("name").avg("")
#df3 = df.select("name", "age", explode("subjects").alias("subject"))
#df3.show()
df3 = df2.select("student", "age", "subject.name", "subject.score")
df4=df3.select("student", "age",df3["name"].alias("subject"),"score")
#df4.show()
df5=df4.groupby("student").avg("score")
#df5.show()
df6 = df4.filter(df4["subject"] == "Math").sort(df4["score"].desc())
#df6.show()

df7 = df4.groupBy("student").pivot("subject").agg(F.avg("score"))
df7.show()

#df3 = df2.select("student", "age", df2.["name"].alias("subject"),"subject.score")
#df3.show()

#df4=df3.withColumnRenamed("subject.name","sub").printSchema()
#
#df4 = df.groupBy("name").agg(F.avg("score").alias("average_score"))
#df4.show()