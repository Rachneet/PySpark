from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, StructType
from pyspark.sql.functions import format_number, col, dayofyear, max, min, sum, corr, year, month

spark = SparkSession.builder.appName("df_exercise").getOrCreate()

df = spark.read.csv("./files/walmart_stock.csv", inferSchema=True, header=True)
# df.show()

# print(df.columns)

# df.printSchema()

# print(df.head(5))

# df.describe().show()

# df = spark.read.csv("./files/walmart_stock.csv", schema=final_struct)
# df.printSchema()
# df.describe().printSchema()
newdf = df.describe()

# data_schema = [StructField("Open", StringType(), True),
#                StructField("High", StringType(), True),
#                StructField("Low", StringType(), True),
#                StructField("Close", StringType(), True),
#                StructField("Volume", StringType(), True),
#                StructField("Adj Close", StringType(), True)]
#
# final_struct = StructType(fields=data_schema)
#
# newdf.show()
# newdf.select([format_number(col(x).cast('double'),2).alias(x) if x != "Date" else newdf['summary']
#               for x in df.columns]).show()


# newdf.select(newdf['summary'],
#               format_number(newdf['Open'].cast('float'),2).alias('Open'),
#               format_number(newdf['High'].cast('float'),2).alias('High'),
#               format_number(newdf['Low'].cast('float'),2).alias('Low'),
#               format_number(newdf['Close'].cast('float'),2).alias('Close'),
#               newdf['Volume'].cast('int').alias('Volume')
#              ).show()

# df.withColumn('HV Ratio', df['High']/df['Volume']).select('HV Ratio').show()

# df.agg({'High':'max'}).show()
#
# df.groupBy().max('High').show()
    # .select(dayofyear(df['Date'])).show()

# df.select(dayofyear('Date')).show()

# print(df.orderBy(df['High'].desc()).head(1)[0][0])

# df.agg({'Close':'avg'}).show()

# df.select(max('Volume'), min('Volume')).show()

# print(df.filter(df['Close'] < 60).count())

# print(df.filter(df['High']>80).count()/df.count()*100)

# df.select(corr('High', 'Volume')).show()

# df.groupBy(year('Date')).max('High').show()

df.groupBy(month('Date')).avg('Close').orderBy(month('Date')).show()