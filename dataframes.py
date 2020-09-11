from pyspark.sql import SparkSession
from pyspark.sql.types import (StructField, StringType,
                               IntegerType, StructType)

spark = SparkSession.builder.appName('Basics').getOrCreate()

# df = spark.read.json("./files/people.json")
# df.show()
# df.printSchema()

# print(df.columns)
# print(df.describe().show())

data_schema = [StructField('age', IntegerType(), True),
               StructField('name', StringType(), True)]
final_struct = StructType(fields=data_schema)

df = spark.read.json("./files/people.json", schema=final_struct)

# df.printSchema()

# select single column
# df.select('age').show()
# print(df.head(2))

# select multiple columns
# df.select(['age', 'name']).show()

# add new column
# not inplace
# df.withColumn('double_age', df['age']*2).show()

# rename column
# df.withColumnRenamed('age', 'new_age').show()

# using sql
df.createOrReplaceTempView('people')
results = spark.sql("SELECT * FROM people")
results.show()

new_results = spark.sql("SELECT * FROM people WHERE age=30")
new_results.show()

