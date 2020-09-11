from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler

spark = SparkSession.builder.appName("consulting").getOrCreate()

df = spark.read.csv("./files/cruise_ship_info.csv", inferSchema=True, header=True)
# df.show()
# df.printSchema()
# df.describe().show()

indexer = StringIndexer(inputCols=['Ship_name', 'Cruise_line'], outputCols=['ship_indexer', 'cruise_indexer'])
result = indexer.fit(df).transform(df)

assembler = VectorAssembler(inputCols=['Tonnage', 'passengers', 'length', 'cabins', 'passenger_density',
                                       'ship_indexer', 'cruise_indexer'],
                            outputCol='features')
output = assembler.transform(result)
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures",
                        withStd=True, withMean=False)
output_scaled = scaler.fit(output).transform(output)

data = output_scaled.select('scaledFeatures', 'crew')
train_data, test_data = data.randomSplit([0.7, 0.3])

lr_model = LinearRegression(featuresCol="scaledFeatures", labelCol="crew")
model = lr_model.fit(train_data)

test_results = model.evaluate(test_data)
# test_results.residuals.show()
print(test_results.rootMeanSquaredError)
print(test_results.r2)

data.describe().show()

# check why model performs so well
from pyspark.sql.functions import corr

df.select(corr('crew', 'passengers')).show()







