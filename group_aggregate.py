from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("group_agg").getOrCreate()

df = spark.read.csv("./files/sales_info.csv", inferSchema=True, header=True)
# df.show()

# df.printSchema()

# sum min max count
# df.groupBy('Company').count().show()
# df.agg({'Sales':'sum'}).show()

# group_data = df.groupBy('Company')
# group_data.agg({'Sales':'max'}).show()

from pyspark.sql.functions import countDistinct,avg,stddev, format_number

# df.select(avg('Sales').alias('Average Sales')).show()

# formatting number
sales_std = df.select(stddev('Sales').alias('std'))
sales_std.select(format_number('std',2).alias('std')).show()

# order by
df.orderBy('Sales').show()  # ascending order
df.orderBy(df['Sales'].desc()).show()  # descending order

