from pyspark.sql import SparkSession
from pyspark.ml.feature import (VectorAssembler, VectorIndexer,
                                OneHotEncoder, StringIndexer)
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = SparkSession.builder.appName("lg_project").getOrCreate()

df = spark.read.csv("./files/customer_churn.csv", inferSchema=True, header=True)
# df.show()
# df.describe().show()
data = df.na.drop()

# indexer = StringIndexer(inputCols=['Location', 'Company'], outputCols=['LocIndex', 'CompIndex'])
# encoder = OneHotEncoder(inputCols=['LocIndex', 'CompIndex'], outputCols=['LocEnc', 'CompEnc'])
#
# out = indexer.fit(data).transform(data)
# out = encoder.fit(out).transform(out)
# out.show()

assembler = VectorAssembler(inputCols=['Age','Total_Purchase','Account_Manager','Years','Num_Sites'],
                            outputCol='features')
out = assembler.transform(data)
train_data, test_data = out.randomSplit([0.7,0.3])

lg_model = LogisticRegression(featuresCol='features', labelCol='Churn')
model = lg_model.fit(train_data)
results = model.evaluate(test_data)

# results.predictions.show()

eval = BinaryClassificationEvaluator(labelCol='Churn', rawPredictionCol='prediction')
auc = eval.evaluate(results.predictions)
print(auc)

final_model = lg_model.fit(out)
new_df = spark.read.csv("./files/new_customers.csv", inferSchema=True, header=True)

new_customers = assembler.transform(new_df)
final_res = final_model.transform(new_customers)

final_res.select(['Company', 'prediction']).show()




