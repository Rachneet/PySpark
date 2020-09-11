from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("spam_filter").getOrCreate()

data = spark.read.csv('./files/SMSSpamCollection', inferSchema=True, sep='\t')
data = data.withColumnRenamed('_c0','class').withColumnRenamed('_c1','text')
# data.show()

from pyspark.sql.functions import length

data = data.withColumn('length', length(data['text']))
data.groupBy('class').mean().show()

from pyspark.ml.feature import (Tokenizer, StopWordsRemover, CountVectorizer,
                                IDF, StringIndexer, VectorAssembler)

tokenizer = Tokenizer(inputCol='text', outputCol='token_text')
stop_remove = StopWordsRemover(inputCol='token_text', outputCol='stop_tokens')
count_vec = CountVectorizer(inputCol='stop_tokens', outputCol='c_vec')
idf = IDF(inputCol='c_vec', outputCol='tf_idf')
ham_spam_to_numeric = StringIndexer(inputCol='class', outputCol='label')

cleaned_set = VectorAssembler(inputCols=['tf_idf', 'length'], outputCol='features')

from pyspark.ml.classification import NaiveBayes
nb = NaiveBayes()

from pyspark.ml import Pipeline
data_prep = Pipeline(stages=[ham_spam_to_numeric, tokenizer,
                             stop_remove, count_vec, idf, cleaned_set])

clean_data = data_prep.fit(data).transform(data)

clean_data = clean_data.select('label', 'features')
clean_data.show()

train_data, test_data = clean_data.randomSplit([0.7,0.3])
spam_model = nb.fit(train_data)
results = spam_model.transform(test_data)
results.show()

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
acc_eval = MulticlassClassificationEvaluator()
acc = acc_eval.evaluate(results)
print(acc)
