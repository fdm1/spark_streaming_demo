from __future__ import print_function

import json
import logging
import os
import re
import sys
from nltk.corpus import stopwords
from operator import add
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
    return {'hashtags': hashes, 'text': text, 'polarity': TextBlob(text).polarity}


def get_word_lists(record):
    custom_stopwords = [w.strip() for w in os.environ.get('custom_stopwords', '').split(',')]
    s=set(stopwords.words('english') + custom_stopwords)
    words = [w for w in TextBlob(record['text']).words if w not in s and len(w) > 2 and '…' not in w]
    return {'words': words, 'polarity': record['polarity']}


if __name__ == "__main__":

    sc = SparkContext(master='spark://master:7077', appName="TwitterStreamConsumer")
    sc.setLogLevel('ERROR')
    kvs = get_stream(sc)
    ssc = kvs.context()
    ssc.checkpoint('/tmp/checkpoint')
    processed = kvs.map(extract_text_and_hashses)
    words = processed.map(get_word_lists)

    
    flat_with_polarity = words.flatMap(lambda x: ([(w, x['polarity']) for w in set(x['words'])]))
    windowed = flat_with_polarity.window(15*60,10)
    grouped_flat = windowed.groupByKey()

    def get_record_stats(record):
        from statistics import mean, stdev, variance
        word = record[0]
        count = len(record[1])
        _mean = mean(record[1])
        _stdev, _variance = None, None
        if count > 1:
            _stdev = stdev(record[1])
            _variance = variance(record[1])
        return {"word": word, "stats": { 'count': count, 'mean': _mean, 'stdev': _stdev, 'variance': _variance}}

    averaged_flat = grouped_flat.map(get_record_stats)
    averaged_flat.transform(lambda rdd: rdd.sortBy(lambda x: x['stats']['count'], ascending=False)).pprint()


    run_stream(kvs.context())
