# THIS DOES NOT WORK YET!


from __future__ import print_function

import sys

import logging
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import os

kafka = 'kafka:9092'
zookeeper = 'zookeeper:2181'
topic = 'tweets'

def create_rdd(sc, host=zookeeper):
    from pyspark.streaming.kafka import OffsetRange
    return KafkaUtils.createRDD(sc, {'metadata.broker.list': host}, [OffsetRange('tweets', 1,0,100)])

def get_stream(sc, host=kafka):
    ssc = StreamingContext(sc, 1)
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list': host})
    return ssc, kvs

def run_stream(ssc):
    ssc.checkpoint("/tmp/checkpoint/")
    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":

    logger = logging.getLogger()
    logger.setLevel(logging.ERROR)

    sc = SparkContext(master='spark://master:7077', appName="TwitterStreamConsumer")
    ssc, kvs = get_stream(sc)

    # kvs.checpoint(1)
    # kvs.saveAsTextFiles(os.path.expanduser('~') + '/tweets', suffix='txt')

    lines = kvs.map(lambda x: x[1])
    lines.pprint() 
    counts = lines.flatMap(lambda line: line.split(" ")) \
                    .map(lambda word: (word, 1)) \
                            .reduceByKey(lambda a, b: a+b)
    logger.error(counts)
    counts.pprint()
    counts.saveAsTextFiles(os.path.expanduser('~') + '/tweets_counts')
    # kvs.pprint()
    # print(kvs)
    run_stream(ssc)
    # def store(rdd):
    #     import time
    # # // Combine each partition's results into a single RDD:
    #     repartitionedRDD = rdd.repartition(1).cache()
    #     # // And print out a directory with the results.
    #     repartitionedRDD.saveAsTextFile("Tweets")
    # # // Stop once we've collected 1000 tweets.
    #



