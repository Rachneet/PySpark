from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler

spark = SparkSession.builder.appName('lreg').getOrCreate()

df = spark.read.csv("./files/Ecommerce_Customers.csv", inferSchema=True, header=True)
# df.printSchema()
# df.show()
assembler = VectorAssembler(inputCols=['Avg Session Length', 'Time on App', 'Time on Website' ,'Length of Membership'],
                            outputCol='features')
output = assembler.transform(df)
final_data = output.select('features', 'Yearly Amount Spent')
# final_data.show()

train_data, test_data = final_data.randomSplit([0.7, 0.3])
# train_data.describe().show()
lrModel = LinearRegression(featuresCol='features', labelCol='Yearly Amount Spent')
model = lrModel.fit(train_data)
test_results = model.evaluate(test_data)
# test_results.residuals.show()
print(test_results.rootMeanSquaredError)
print(test_results.r2)

unlabeled_data = test_data.select('features')
prediction = model.transform(unlabeled_data)
prediction.show()