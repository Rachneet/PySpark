from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("miss").getOrCreate()

df = spark.read.csv("./files/ContainsNull.csv", inferSchema=True, header=True)
# df.show()

# df.na.drop(thresh=2).show()  # atleast 2 non-null to drop
# df.na.drop(how='any').show()
# df.na.drop(how='all').show()
# df.na.drop(subset=['Sales']).show()  # only sales data missing

# fill missing values
df.na.fill('No Name', subset=['Name']).show()

from pyspark.sql.functions import mean

mean_val = df.select(mean(df['Sales'])).collect()
mean_sales = mean_val[0][0]
df.na.fill(mean_sales,'Sales').show()

# one liner
df.na.fill(df.select(mean(df['Sales'])).collect()[0][0],['Sales']).show()

