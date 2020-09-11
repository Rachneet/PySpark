from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, RegexTokenizer, StopWordsRemover
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("nlp").getOrCreate()

sen_df = spark.createDataFrame([
    (0, 'Hi i learn spark'),
    (1, 'Learning is everything'),
    (2,'Model,regress,classify')
], ['id', 'sentence'])

sen_df.show()

# based on whitespace
tokenizer = Tokenizer(inputCol='sentence', outputCol='words')
# based on pattern
regex_tokenizer = RegexTokenizer(inputCol='sentence', outputCol='words',
                                 pattern='\\W')

count_tokens = udf(lambda words : len(words), IntegerType())
tokenized = tokenizer.transform(sen_df)
# tokenized.withColumn('tokens', count_tokens(col('words'))).show()

rg_tokenized = regex_tokenizer.transform(sen_df)
# rg_tokenized.withColumn('tokens', count_tokens(col('words'))).show()
#
# sentence_df = spark.createDataFrame([
#     (0, ['I', 'saw', 'the', 'green', 'horse']),
#     (1, ['Mary', 'had', 'a', 'little', 'lamb']),
# ], ['id', 'tokens'])
#
# remover = StopWordsRemover(inputCol='tokens', outputCol='filtered')
# remover.transform(sentence_df).show()
#
# # n-gram
# from pyspark.ml.feature import NGram
#
# ngram = NGram(n=2, inputCol='tokens', outputCol='grams')
# ngram.transform(sentence_df).show()
# ngram.transform(sentence_df).select('grams').show(truncate=False)

from pyspark.ml.feature import HashingTF, IDF, CountVectorizer

hashing_tf = HashingTF(inputCol='words', outputCol='rawFeatures')
featurized_data = hashing_tf.transform(rg_tokenized)
# featurized_data.show()

idf = IDF(inputCol='rawFeatures', outputCol='features')
idf_model = idf.fit(featurized_data).transform(featurized_data)

idf_model.select('id', 'features').show()

# count vectorizer
df = spark.createDataFrame([
    (0, "a b c".split(" ")),
    (1, "a b b c a".split(" ")),
], ['id', 'words'])
cv = CountVectorizer(inputCol='words', outputCol='features',
                     vocabSize=3, minDF=2.0)
model = cv.fit(df).transform(df)

model.show(truncate=False)