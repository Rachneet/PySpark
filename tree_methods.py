from pyspark.sql import SparkSession
from pyspark.ml.feature import (VectorAssembler, VectorIndexer,
                                OneHotEncoder, StringIndexer)
from pyspark.ml.classification import (DecisionTreeClassifier, RandomForestClassifier,
                                       GBTClassifier)
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

spark = SparkSession.builder.appName("trees").getOrCreate()

df = spark.read.csv("./files/College.csv", inferSchema=True, header=True)
df.show()
df.printSchema()
print(df.columns)

assembler = VectorAssembler(inputCols=['Apps', 'Accept', 'Enroll', 'Top10perc', 'Top25perc', 'F_Undergrad',
                                       'P_Undergrad', 'Outstate', 'Room_Board', 'Books', 'Personal', 'PhD', 'Terminal',
                                       'S_F_Ratio', 'perc_alumni', 'Expend', 'Grad_Rate'],
                            outputCol='features')

output = assembler.transform(df)
indexer = StringIndexer(inputCol='Private', outputCol='PrivateIndex')
output_fixed = indexer.fit(output).transform(output)

# output_fixed.printSchema()

final_data = output_fixed.select(['features', 'PrivateIndex'])
train_data, test_data = final_data.randomSplit([0.7,0.3])

dtc = DecisionTreeClassifier(labelCol='PrivateIndex', featuresCol='features')
rfc = RandomForestClassifier(labelCol='PrivateIndex', featuresCol='features')
gbt = GBTClassifier(labelCol='PrivateIndex', featuresCol='features')

dtc_model = dtc.fit(train_data)
rfc_model = rfc.fit(train_data)
gbt_model = gbt.fit(train_data)

dtc_preds = dtc_model.transform(test_data)
rfc_preds = rfc_model.transform(test_data)
gbt_preds = gbt_model.transform(test_data)

my_binary_eval = BinaryClassificationEvaluator(labelCol='PrivateIndex')
print("DTC")
print(my_binary_eval.evaluate(dtc_preds))

print("RFC")
print(my_binary_eval.evaluate(rfc_preds))

# gbt only has a prediction column
gbt_eval = BinaryClassificationEvaluator(labelCol='PrivateIndex',
                                         rawPredictionCol='prediction')
print("GBT")
print(gbt_eval.evaluate(gbt_preds))

acc_eval = MulticlassClassificationEvaluator(labelCol='PrivateIndex',
                                             metricName='accuracy')

rfc_acc = acc_eval.evaluate(rfc_preds)
print(rfc_acc)