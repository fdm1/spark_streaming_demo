from __future__ import print_function

import itertools
import json
import logging
import os
import re
import sys
from nltk.collocations import BigramCollocationFinder
from nltk.corpus import stopwords
from nltk.metrics import BigramAssocMeasures
from operator import add
from pyspark import SparkContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, BooleanType
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import OffsetRange
from textblob import TextBlob
from uuid import uuid4


KAFKA_CONNECT = 'kafka:9092'
KAFKA_TOPIC = 'tweets'
WINDOW_MINUTES = int(os.environ.get('streaming_window_minutes', 5))
SLIDE_SECONDS = int(os.environ.get('streaming_window_slide_seconds', 30))
WINDOW_PARAMS = [WINDOW_MINUTES*60,SLIDE_SECONDS]

DATA_FRAME_SCHEMAS = {
    'flatmapped_tweets': 
        StructType([
            StructField("word", StringType(), True),
            # StructField("count", IntegerType(), True),
            StructField("polarity", DoubleType(), True),
            # StructField("mean", DoubleType(), True),
            # StructField("stdev", DoubleType(), True),
            # StructField("variance", DoubleType(), True),
            StructField("uuid", StringType(), True),
            StructField("hashtag", StringType(), True),
            StructField("retweet", BooleanType(), True),
        ]),
}


def create_rdd(sc, max_offset=100, partition=0, min_offset=0):
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
    stream = KafkaUtils.createDirectStream(ssc, [KAFKA_TOPIC], {'metadata.broker.list': KAFKA_CONNECT})
    return stream


def run_stream(ssc):
    """Run the stream"""
    ssc.start()
    ssc.awaitTermination()


# http://spark.apache.org/docs/latest/streaming-programming-guide.html#dataframe-and-sql-operations
# Lazily instantiated global instance of SparkSession
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]


def clean_text(text):
    """lower tweets, remove retweet identifier, hashes, and mentions"""
    text = text.lower()
    text = re.sub(r'^rt ', '', text)  # remove RT at beginning of text
    text = re.sub(r'@\S*', '', text)  #remove mentions
    text = re.sub(r'#\S*', '', text)  #remove hashtags
    text = re.sub(r'http\S*', '', text)  # remove links
    text = re.sub(r'http\S*', '', text)  # remove links
    text = re.sub(r'[^a-zA-Z0-9 ]', '', text) # remove non-letters/numbers
    return text


def extract_text_and_hashses(record):
    """Print the tweet count and basic stats about polarity of the tweets"""
    json_record = json.loads(record[1])
    text =  json_record['text']
    retweet =  json_record['retweet']
    hashes = [h.lower() for h in re.findall(r'#\S*', text)]
    text= clean_text(text)
    text_blob = TextBlob(text)
    return {'hashtags': hashes or ['NONE'],
            'text': text,
            'retweet': retweet,
            'polarity': TextBlob(text).polarity}


def get_word_lists(record):
    custom_stopwords = [w.strip() for w in os.environ.get('custom_stopwords', '').split(',')]
    s=set(stopwords.words('english') + custom_stopwords)
    words = [w for w in TextBlob(record['text']).words if w not in s and len(w) > 2 and '…' not in w]
    return {'words': words,
            'polarity': record['polarity'],
            'hashtags': record['hashtags'],
            'retweet': record['retweet'],
            'uuid': str(uuid4())}


def flatmap_words_and_hashes(record):
    return ([{'word': w.string, 
              'polarity': record['polarity'],
              'retweet': record['retweet'],
              'hashtag': h,
              'uuid': record['uuid']} 
      for w in set(record['words'])
      for h in set(record['hashtags'])
    ])


def get_rows(record):
    return Row(**record)
#
# def get_record_stats(record):
#     from statistics import mean, stdev, variance
#     word = record['word']
#     polarity = record['polarity']
#     count = len(polarity)
#     _mean = mean(polarity)
#     _stdev, _variance = None, None
#     if count > 1:
#         _stdev = stdev(polarity)
#         _variance = variance(polarity)
#     return Row(word=word,
#                count=count,
#                mean= _mean,
#                stdev= _stdev,
#                variance= _variance,
#                hashtag=record['hashtag'],
#                uuid=record['uuid'],
#                retweet=record['retweet'])
#     

def wrapper_line():
    print("="*30 + "\n")

def bigram_word_feats(words, score_fn=BigramAssocMeasures.chi_sq, n=200):
    bigram_finder = BigramCollocationFinder.from_words(words)
    bigrams = bigram_finder.nbest(score_fn, n)
    return dict([(ngram, True) for ngram in itertools.chain(words, bigrams)])

def process_stream(time, rdd):
    wrapper_line()
    print(time)
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        print("Window tweet count: {}".format(rdd.count()))
        print("\n")

        processed_text_RDD = rdd.map(extract_text_and_hashses)
        word_list_RDD = processed_text_RDD.map(get_word_lists)
        flatmapped_RDD = word_list_RDD.flatMap(flatmap_words_and_hashes)
        # grouped_flat_RDD = flatmapped_RDD.groupBy(lambda x: x['word'])
        # word_polarity_stats_RDD = grouped_flat_RDD.map(get_record_stats)

        # Creates a temporary view using the DataFrame
        df = spark.createDataFrame(flatmapped_RDD.map(get_rows), DATA_FRAME_SCHEMAS['flatmapped_tweets'])
        pdf = df.toPandas()
        pdf.groupby('word').sum().sort_index(by='counter', ascending=False)
        # tweets = df.select(['uuid', 'polarity', 'retweet']).dropDuplicates()
        # tweets.createOrReplaceTempView('tweets')
        #
        # hashtags = df.select(['uuid', 'hashtag']).dropDuplicates()
        # hashtags.createOrReplaceTempView('hashtags')
        #
        # words = df.select(['uuid', 'word']).dropDuplicates()
        # words.createOrReplaceTempView('words')
        #
        # top_words = spark.sql('select word, count(1) as count from words group by 1 order by 2 desc limit 20')
        #
        # top_words.join(words, on='word', how='inner')
        #
        # words.crossJoin(tweets.select('uuid', 'polarity')).select('word', 'polarity').show()
        # hashtags.crossJoin(tweets.select('uuid', 'polarity')).select('hashtag', 'polarity').show()
        # top_word_hashtags = top_words.crossJoin(hashtags.select('uuid', 'hashtag')).select('uuid', 'word', 'hashtag')
        #
        #
# ideas:
# - get timestamp
# - min/max/mean of polarity
# - use top 10000 words filter
# - use filtered to measure polarity
# - transform to root of word

        window_count = tweets.count()
        

    except Exception as e:
        print(str(e))
        pass

    finally:
        wrapper_line()


if __name__ == "__main__":

    sc = SparkContext(master='spark://master:7077', appName="TwitterStreamConsumer")
    sc.setLogLevel('ERROR')
    stream = get_stream(sc)
    ssc = stream.context()
    ssc.checkpoint('/tmp/checkpoint')
    stream.window(*WINDOW_PARAMS).foreachRDD(process_stream)
    # flat_with_hashes = windowed.flatMap(lambda x: ([(w, x['hashtags']) for w in set(x['words'])])).map(lambda x: (x[0], ['NONE']) if not x[1] else x).flatMap(lambda x: ((x[0], h) for h in x[1])).groupByKey().map(lambda x: {'word': x[0], 'hashes': x[1]})

    run_stream(stream.context())
