from __future__ import print_function

import json
import logging
import os
import re
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


def extract_text(record):
    """Print the tweet count and basic stats about polarity of the tweets"""
    return json.loads(record[1])['text']


def clean_text(text):
    text = re.sub(r'^RT ', '', text)  # remove RT at beginning of text
    text = re.sub(r'@\S*', '', text)  #remove mentions
    text = re.sub(r'#\S*', '', text)  #remove hashtags
    text = re.sub(r'http\S*', '', text)  # remove links
    return text


def extract_noun_phrases(cleaned_text):
    return TextBlob(cleaned_text).noun_phrases  # return noun_phrases for each cleaned tweet


def run_stream(ssc):
    """Run the stream"""
    ssc.checkpoint("/tmp/checkpoint/")
    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":

    sc = SparkContext(master='spark://master:7077', appName="TwitterStreamConsumer")
    sc.setLogLevel('ERROR')
    kvs = get_stream(sc)

    # window_stream = kvs.window(60,10)

    text_stream = kvs.map(extract_text)
    cleaned_stream = text_stream.map(clean_text)
    noun_phrases = cleaned_stream.map(extract_noun_phrases)

    # text_stream.pprint()
    # cleaned_stream.pprint()
    noun_phrases.window(60,10).pprint()

    run_stream(kvs.context())
