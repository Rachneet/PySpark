import findspark
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# print(findspark.find())
findspark.init('/home/rachneet/spark-3.0.1-bin-hadoop2.7')
sc = SparkContext('local[2]', 'NetworkWordCount')
ssc = StreamingContext(sc, 1)  # get data at interval of 1 sec

lines = ssc.socketTextStream('localhost', 9999)
words = lines.flatMap(lambda line: line.split(' '))
pairs = words.map(lambda word: (word,1))
word_counts = pairs.reduceByKey(lambda num1, num2: num1+num2)
word_counts.pprint()
ssc.start()
