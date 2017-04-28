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


KAFKA_CONNECT = 'kafka:9092'
KAFKA_TOPIC = 'tweets'
WINDOW_MINUTES = int(os.environ.get('streaming_window_minutes', 5))
WINDOW_SLIDING_SECONDS = int(os.environ.get('streaming_window_slide_seconds', 30))


def create_rdd(sc, partition=0, min_offset=0, max_offset=1):
    """Create an RDD from a subset of the kafka stream.
    For use in pyspark shell to get some of the stream and inspect"""
    offset = OffsetRange(KAFKA_TOPIC, partition, min_offset, max_offset)
    return KafkaUtils.createRDD(sc, {'metadata.broker.list': KAFKA_CONNECT }, [offset])


def get_scc(sc, batch_interval):
    """Get a StreamingContext object"""
    return StreamingContext(sc, batch_interval)


def get_stream(sc, batch_interval=1):
    """Create the Kafka DirectStream"""
    ssc = get_scc(sc, batch_interval)
    kvs = KafkaUtils.createDirectStream(ssc, [KAFKA_TOPIC], {'metadata.broker.list': KAFKA_CONNECT})
    return kvs


def run_stream(ssc):
    """Run the stream"""
    ssc.start()
    ssc.awaitTermination()


def clean_text(text):
    """lower tweets, remove retweet identifier, hashes, and mentions"""
    text = text.lower()
    text = re.sub(r'^rt ', '', text)  # remove RT at beginning of text
    text = re.sub(r'@\S*', '', text)  #remove mentions
    text = re.sub(r'#\S*', '', text)  #remove hashtags
    text = re.sub(r'http\S*', '', text)  # remove links
    return text


def extract_text_and_hashses(record):
    """Print the tweet count and basic stats about polarity of the tweets"""
    text =  json.loads(record[1])['text']
    hashes = [h.lower() for h in re.findall(r'#\S*', text)]
    text= clean_text(text)
    text_blob = TextBlob(text)
    return {'hashtags': hashes,
            'text': text,
            'polarity': text_blob.polarity}


if __name__ == "__main__":

    sc = SparkContext(master='spark://master:7077', appName="TwitterStreamConsumer")
    sc.setLogLevel('ERROR')
    kvs = get_stream(sc)

    # Clean text and extract hashes, and then split DStream into winows for further processing
    processed = kvs.map(extract_text_and_hashses) \
                   .window(WINDOW_MINUTES*60,WINDOW_SLIDING_SECONDS)

    # Print a random sample to the console
    sample = processed.transform(lambda rdd: rdd.sample(False, (1.0/3)))
    sample.pprint()

    # Get polarity of each cleaned tweet and print stats to the console
    polarity = processed.map(lambda record: record['polarity'])
    polarity.foreachRDD(lambda rdd: print(rdd.stats()))
    
    # get word lists of cleaned text and print to the console
    words = processed \
            .map(lambda record: record['text']) \
            .map(lambda text: [w for w in TextBlob(text).words \
                                 if len(w) > 2 \
                                 and w not in ['the', 'and']])
    words.pprint()

    run_stream(kvs.context())
