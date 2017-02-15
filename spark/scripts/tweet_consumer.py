from __future__ import print_function

import json
import logging
import os
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import OffsetRange
from textblob import TextBlob


kafka = 'kafka:9092'
topic = 'tweets'


def create_rdd(sc, host=kafka, topic=topic, partition=0, min_offset=0, max_offset=1):
    """Create an RDD from a subset of the kafka stream.
    For use in pyspark shell to get some of the stream and inspect"""
    offset = OffsetRange(topic, partition, min_offset, max_offset)
    return KafkaUtils.createRDD(sc, {'metadata.broker.list': host}, [offset])


def get_scc(sc, batch_interval):
    """Get a StreamingContext object"""
    return StreamingContext(sc, batch_interval)


def get_stream(sc, host=kafka, batch_interval=1):
    """Create the Kafka DirectStream"""
    ssc = get_scc(sc, batch_interval)
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list': host})
    return kvs


def get_polarity(record):
    """Return the polarity of each tweet"""
    return TextBlob(record).sentiment.polarity


def print_rdd(rdd):
    """Print the tweet count and basic stats about polarity of the tweets"""
    datamap = rdd.map(lambda x: json.loads(x[1]))
    print("\n\n{} Tweets\n__________\n".format(datamap.count()))
    polarity = datamap.map(get_polarity)
    print("Polarity:\nmin: {}, mean: {}, max: {}\n__________\n".format(polarity.min(), polarity.mean(), polarity.max()))


def run_stream(ssc):
    """Run the stream"""
    ssc.checkpoint("/tmp/checkpoint/")
    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":

    sc = SparkContext(master='spark://master:7077', appName="TwitterStreamConsumer")
    sc.setLogLevel('ERROR')
    kvs = get_stream(sc)
    kvs.pprint()  # print 10 tweets at a time as they come in.
    kvs.window(10,5).foreachRDD(print_rdd)  # In 10 second batches (rolling every 5 seconds), print stats
    run_stream(kvs.context())






    # # kvs.saveAsTextFiles(os.path.expanduser('~') + '/tweets', suffix='txt')
    #
    # # kvs.pprint()
    # # counts.pprint()
    # # print(dir(c))
    #
    # # kvs.checkpoint(5)
    # avgs = kvs.mapValues(lambda v: (v, 1)) \
    #             .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))
    # avgs.pprint()
    # lines.pprint()
    # counts = lines.flatMap(lambda line: line.split(" ")) \
    #                 .map(lambda word: (word, 1)) \
    #                         .reduceByKey(lambda a, b: a+b)
    # # logger.error(counts)
    # counts.saveAsTextFiles(os.path.expanduser('~') + '/tweets_counts')
    #

    # counts.pprint()
    # kvs.pprint()
    # print(kvs)
    # def store(rdd):
    #     import time
    # # // Combine each partition's results into a single RDD:
    #     repartitionedRDD = rdd.repartition(1).cache()
    #     # // And print out a directory with the results.
    #     repartitionedRDD.saveAsTextFile("Tweets")
    # # // Stop once we've collected 1000 tweets.
    #



