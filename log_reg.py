from pyspark.sql import SparkSession
from pyspark.ml.feature import (VectorAssembler, VectorIndexer,
                                OneHotEncoder, StringIndexer)
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = SparkSession.builder.appName("titanic").getOrCreate()

df = spark.read.csv("./files/titanic.csv", inferSchema=True, header=True)
df.show()

my_cols = df.select(['Survived', 'Pclass', 'Sex', 'Age', 'SibSp', 'Parch', 'Fare', 'Embarked'])
final_data = my_cols.na.drop()

gender_idx = StringIndexer(inputCol='Sex', outputCol='SexIndex')
gender_enc = OneHotEncoder(inputCol='SexIndex', outputCol='SexVec')

embark_idx = StringIndexer(inputCol='Embarked', outputCol='EmbarkIndex')
embark_enc = OneHotEncoder(inputCol='EmbarkIndex', outputCol='EmbarkVec')

assembler = VectorAssembler(inputCols=['Pclass', 'SexVec', 'EmbarkVec', 'Age', 'SibSp', 'Parch', 'Fare'],
                            outputCol='features')

log_reg = LogisticRegression(featuresCol='features', labelCol='Survived')

pipeline = Pipeline(stages=[
    gender_idx, embark_idx, gender_enc, embark_enc,
    assembler, log_reg
])

train_data, test_data = final_data.randomSplit([0.7,0.3])

model = pipeline.fit(train_data)
results = model.transform(test_data)

my_eval = BinaryClassificationEvaluator(rawPredictionCol='prediction',
                                        labelCol='Survived')
# see results
auc = my_eval.evaluate(results)
print(auc)










