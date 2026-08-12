import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PulseTest") \
    .master("local[*]") \
    .getOrCreate()

print("Spark version:", spark.version)

data = [("Hello Spark",), ("PULSE project test",)]
df = spark.createDataFrame(data, ["text"])
df.show()

spark.stop()