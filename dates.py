from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("dates").getOrCreate()

df = spark.read.csv("./files/appl_stock.csv", inferSchema=True, header=True)
# df.select(['Date', 'Open']).show()

from pyspark.sql.functions import (dayofmonth, hour,
                                   dayofyear, month,
                                   year, weekofyear,
                                   format_number, date_format)
# df.select(dayofmonth(df['Date'])).show()

newdf = df.withColumn("Year",year(df['Date']))
result = newdf.groupBy('Year').mean().select(['Year','avg(Close)'])
result.show()

new = result.withColumnRenamed("avg(Close)","Average Closing Price")
new.select(['Year',format_number("Average Closing Price",2).alias('Average Close')]).show()