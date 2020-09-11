from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("basic_ops").getOrCreate()

df = spark.read.csv("./files/appl_stock.csv", inferSchema=True, header=True)
# df.printSchema()

# df.show()

# df.filter("Close < 500").show()
# df.filter("Close < 500").select(['Open', 'Close']).show()
# df.filter(df['Close'] < 500).show()
# df.filter((df['Close'] < 200) & ~(df['Open'] > 200)).show()

df.filter(df['Low'] == 197.16).show()

# collect results in a list
res = df.filter(df['Low'] == 197.16).collect()
row = res[0]
print(row.asDict()['Volume'])
