from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.appName('recommender').getOrCreate()

df = spark.read.csv('./files/movielens_ratings.csv', inferSchema=True, header=True)
# df.show()
# df.describe().show()

train_data, test_data = df.randomSplit([0.8,0.2])
als = ALS(userCol='userId', itemCol='movieId', ratingCol='rating')
model = als.fit(train_data)
preds = model.transform(test_data)
# preds.show()

eval = RegressionEvaluator(metricName='rmse', labelCol='rating',
                           predictionCol='prediction')
rmse = eval.evaluate(preds)
print(rmse)

single_user = test_data.filter(test_data['userId']==11).select(['movieId','userId'])
single_user.show()

recommend = model.transform(single_user)
recommend.orderBy('prediction', ascending=False).show()