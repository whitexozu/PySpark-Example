from pyspark import SparkContext, SparkConf, storagelevel
from pyspark.streaming.context import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

conf = SparkConf()
sc = SparkContext(master="local[*]", appName="KafkaSample", conf=conf)
# sc = SparkContext(master="yarn", appName="KafkaSample", conf=conf)
ssc = StreamingContext(sc, 3)

ds1 = KafkaUtils.createStream(ssc, "localhost:2181", "test-consumer-group1", {"test": 3})
ds2 = KafkaUtils.createDirectStream(ssc, ["test"], {"metadata.broker.list": "localhost:9092"})

ds1.pprint()
ds2.pprint()

ssc.start()
ssc.awaitTermination()

# spark-submit --master yarn --jars spark-streaming-kafka-0-8-assembly_2.11-2.1.1.jar kafka_sample.py