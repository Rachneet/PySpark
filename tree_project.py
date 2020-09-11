from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator


spark = SparkSession.builder.appName("tree_project").getOrCreate()
df = spark.read.csv("./files/dog_food.csv", inferSchema=True, header=True)

# df.show()
# df.printSchema()

assembler = VectorAssembler(inputCols=['A', 'B', 'C', 'D'], outputCol='features')
output = assembler.transform(df)

train_data, test_data = output.randomSplit([0.7, 0.3])
rfc_model = RandomForestClassifier(featuresCol='features', labelCol='Spoiled')
model = rfc_model.fit(train_data)
result = model.transform(test_data)

# result.show()

eval = BinaryClassificationEvaluator(labelCol='Spoiled')
auc = eval.evaluate(result)
print(auc)

print(model.featureImportances)