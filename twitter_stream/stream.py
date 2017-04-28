import json
import logging
import os
import sys
import tweepy
from kafka import KafkaProducer
from kafka.errors import BrokerNotAvailableError, NoBrokersAvailable
from time import sleep

consumer_token = os.environ['consumer_token']
consumer_secret = os.environ['consumer_secret']
access_token = os.environ['access_token']
access_token_secret = os.environ['access_token_secret']


# http://docs.tweepy.org/en/v3.5.0/streaming_how_to.html
#override tweepy.StreamListener to add logic to on_status
class MyStreamListener(tweepy.StreamListener):

    def __init__(self, filter_terms=None):
        self.logger = logging.getLogger(__file__)
        self.logger.setLevel(logging.WARN)

        self.filters = dict(async=True)
        if filter_terms:
            self.filters.update(track=filter_terms)
        else:
            # at a minimum, enforce only tweets with geolocation
            self.filters.update(locations=[-180,-90,180,90])

        self.api = self.get_auth_api()
        self.producer = None
        while not self.producer:
             self.initialize_kafka_producer()
        self.run_stream()

    def initialize_kafka_producer(self):
        self.logger.warn('Attempting to initialize Kafka Producer')
        try:
            self.producer = KafkaProducer(bootstrap_servers=os.environ['KAFKA_SERVER'],
                                          value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            self.logger.warn('Kafka Producer initialized!')
        except (BrokerNotAvailableError, NoBrokersAvailable):
            self.logger.warn('Kafka server not ready yet')
            sleep(2)
            return False

    # http://docs.tweepy.org/en/v3.5.0/auth_tutorial.html#auth-tutorial
    def get_auth_api(self):
        auth = tweepy.OAuthHandler(consumer_token, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        return tweepy.API(auth)

    def on_data(self, data):
        # print("data: {}".format(data))
        text = json.loads(data).get(u'text')
        # self.logger.warn(data)
        if text:
            self.producer.send('tweets', text)
            self.producer.flush()
            # print(text)
            # self.logger.warn(text)

    def on_error(self, status_code):
        self.logger.warn("TwitterStreamError: {}".format(status_code))
        if status_code == 420:
            #returning False in on_data disconnects the stream
            return False

    def on_connect(self):
        self.logger.warn('Stream Initialized!')

    def on_limit(self, track):
        self.logger.warn('Stream is being limited: {}'.format(track))

    def on_exception(self, exception):
        self.logger.warn('Stream raised exception: {}'.format(exception))

    def run_stream(self):
        self.logger.warn('Initializing stream')
        stream = tweepy.Stream(auth = self.api.auth, listener=self)
        stream.filter(**self.filters)

if __name__ == '__main__':
    MyStreamListener(sys.argv[1:])

