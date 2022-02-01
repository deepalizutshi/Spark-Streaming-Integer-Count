from pyspark import SparkContext
from pyspark.streaming import StreamingContext
sc=SparkContext("local[2]","IntegerCount")
ssc=StreamingContext(sc,1)
lines=ssc.socketTextStream("localhost", 62013)
integers = lines.flatMap(lambda line: line.split(","))
pairs = integers.map(lambda x: float(x)).map(lambda x: round(x)).map(lambda x: (x, 1))
intCounts = pairs.reduceByKey(lambda x, y: x + y)
intCounts.pprint()
ssc.start()
ssc.awaitTermination()



#pairs = integers.map(lambda x: float(x)).map(lambda x: round(x)).map(lambda x: (x, 1))