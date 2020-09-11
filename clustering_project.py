from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler

spark = SparkSession.builder.appName("clustering_project").getOrCreate()
df = spark.read.csv("./files/hack_data.csv", inferSchema=True, header=True)
# df.show()

assembler = VectorAssembler(inputCols=['Session_Connection_Time', 'Bytes Transferred', 'Kali_Trace_Used',
                                      'Servers_Corrupted', 'Pages_Corrupted', 'WPM_Typing_Speed'],
                            outputCol='features')

data = assembler.transform(df)
scaler = StandardScaler(inputCol='features', outputCol='scaledFeatures')
scaled_data = scaler.fit(data).transform(data)

kmeans = KMeans(featuresCol='scaledFeatures', k=2)
model = kmeans.fit(scaled_data)
print(model.clusterCenters())

# model.transform(scaled_data).select('prediction').show()
model.transform(scaled_data).groupBy('prediction').count().show()