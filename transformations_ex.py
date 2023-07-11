import os
import sys
from pyspark import SparkContext
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
from pyspark.sql import SparkSession
# MAP-map (map()) is an RDD transformation that is used to apply the transformation function (lambda)
# on every element of RDD/DataFrame and returns a new RDD.output of map transformations would always have the same number
# of records as input.
spark = SparkSession.builder.appName("basic transformations").master("local[*]").getOrCreate()
sc = spark.sparkContext
x= sc.parallelize(["b", "a", "c"])
y= x.map(lambda z: (z, 1))
print(x.collect())
print(y.collect())

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = ["Project",
"Gutenberg’s",
"Alice’s",
"Adventures",
"in",
"Wonderland",
"Project",
"Gutenberg’s",
"Adventures",
"in",
"Wonderland",
"Project",
"Gutenberg’s"]

rdd=spark.sparkContext.parallelize(data)

rdd2=rdd.map(lambda x: (x,1))
for element in rdd2.collect():
    print(element)

# FLAT-MAP-a transformation operation that flattens the RDD/DataFrame (array/map DataFrame columns) after applying the
# function on every element and returns a new PySpark RDD/DataFrame.
import os
import sys
from pyspark import SparkContext
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("basic transformations").master("local[*]").getOrCreate()
sc = spark.sparkContext
x= sc.parallelize([1,2,3])
y= x.flatMap(lambda x: (x, x*100, 42))
print(x.collect())
print(y.collect())

data = ["Project Gutenberg’s",
        "Alice’s Adventures in Wonderland",
        "Project Gutenberg’s",
        "Adventures in Wonderland",
        "Project Gutenberg’s"]
rdd=spark.sparkContext.parallelize(data)
for element in rdd.collect():
    print(element)

#Flatmap
rdd2=rdd.flatMap(lambda x: x.split(" "))
for element in rdd2.collect():
    print(element)

#group by key
import os
import sys
from pyspark import SparkContext
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("basic transformations").master("local[*]").getOrCreate()
sc = spark.sparkContext

x= sc.parallelize([('B',5),('B',4),('A',3),('A',2),('A',1)])
y= x.groupByKey()
print(x.collect())
print(list((j[0], list(j[1])) for j in y.collect()))


#UNION
import os
import sys
from pyspark import SparkContext
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("basic transformations").master("local[*]").getOrCreate()
sc = spark.sparkContext
x= sc.parallelize([1,2,3], 2)
print(x)
y= sc.parallelize([3,4], 1)
print(y)
z= x.union(y)
print(z.glom().collect())
