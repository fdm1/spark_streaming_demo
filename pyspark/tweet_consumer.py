# THIS DOES NOT WORK YET!


from __future__ import print_function

import sys

import logging
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


def get_stream(sc):
    ssc = StreamingContext(sc, 1)
    zookeeper = 'zookeeper:2181'
    topic = 'tweets'
    # createDirectStream breaks...why?
    # kvs = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list': zookeeper})

    kvs = KafkaUtils.createStream(ssc,
                                  zookeeper,
                                  'twitter-consumer',
                                  {'tweets': 1})
    return ssc, kvs


def run_stream(ssc):
    # ssc.checkpoint("/tmp/checkpoint/")
    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":

    logger = logging.getLogger()
    logger.setLevel(logging.ERROR)

    sc = SparkContext(master='spark://master:7077', appName="TwitterStreamConsumer")
    ssc, kvs = get_stream(sc)

    # kvs.checpoint(1)
    kvs.pprint(10)
    run_stream(ssc)
    # def store(rdd):
    #     import time
    # # // Combine each partition's results into a single RDD:
    #     repartitionedRDD = rdd.repartition(1).cache()
    #     # // And print out a directory with the results.
    #     repartitionedRDD.saveAsTextFile("Tweets")
    # # // Stop once we've collected 1000 tweets.
    #
    # kvs.foreachRDD(store)
    # lines = kvs.map(lambda x: str(x))
    # lines.pprint() 
    # LOG.warn(lines)
    # counts = lines.flatMap(lambda line: line.split(" ")) \
    #                 .map(lambda word: (word, 1)) \
    #                         .reduceByKey(lambda a, b: a+b)
    # logger.error(counts)
    # counts.pprint()



