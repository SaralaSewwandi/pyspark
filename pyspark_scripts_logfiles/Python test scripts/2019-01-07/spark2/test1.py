# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession

# Create my_spark
my_spark = SparkSession.builder.getOrCreate()

print("======================================")
# Print my_spark
print(my_spark)

print("=======================================")
