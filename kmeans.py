from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator


spark = SparkSession.builder.appName("clustering").getOrCreate()
df = spark.read.csv("./files/seeds_dataset.csv", inferSchema=True, header=True)

# df.show()

assembler = VectorAssembler(inputCols=df.columns,
                            outputCol='features')

data = assembler.transform(df)
scaler = StandardScaler(inputCol='features', outputCol='scaledFeatures')
scaled_data = scaler.fit(data).transform(data)
kmeans = KMeans(featuresCol='scaledFeatures').setK(3)
model = kmeans.fit(scaled_data)

# print("WSSSE")
# print(model.computeCost(scaled_data))
print(model.clusterCenters())

model.transform(scaled_data).select('prediction').show()

