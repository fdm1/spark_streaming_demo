# THIS DOES NOT WORK YET!


from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# if __name__ == "__main__":
#     if len(sys.argv) != 3:
#         print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
#         exit(-1)

# sc = SparkContext(appName="PythonStreamingKafkaWordCount")
ssc = StreamingContext(sc, 1)


zookeeper = os.environ.get('KAFKA_ZOOKEEPER_CONNECT')
kafka = os.environ.get('kafka:9092')
topic = 'tweets'
# zkQuorum, topic = sys.argv[1:]
kvs = KafkaUtils.createStream(ssc, zkQuorum=zookeeper, groupId='foo', topics={'tweets': 1})

#         kafka, "twitter-stream-consumer", {topic: 1})
# kvs = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list': zookeeper})
# kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
# lines = kvs.map(lambda x: x[1])
# counts = lines.flatMap(lambda line: line.split(" ")) \
#     .map(lambda word: (word, 1)) \
#     .reduceByKey(lambda a, b: a+b)
# counts.pprint()
#
ssc.start()
ssc.awaitTermination()
