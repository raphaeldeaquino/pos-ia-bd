import configparser
import logging
from queue import Queue
from tweepy import Stream
from confluent_kafka import Producer
import atexit


config = configparser.ConfigParser()
config.read('twitter.env')
consumer_key = config.get('Twitter', 'TWITTER_API_KEY')
consumer_secret = config.get('Twitter', 'TWITTER_SECRET_KEY')
token = config.get('Twitter', 'TWITTER_TOKEN')
secret = config.get('Twitter', 'TWITTER_TOKEN_SECRET')


def exit_handler():
    logging.info("stopping application...")
    logging.info("shutting down client from twitter...")
    logging.info("closing producer...")
    logging.info("done!")


def callback(error, record_metadata):
    if error:
        logging.error("Something bad happened " + error)


class StdOutListener(Stream):
    msg_queue = None

    def __init__(self, msg_queue, consumer_key, consumer_secret, token, secret):
        Stream.__init__(self, consumer_key, consumer_secret, token, secret)
        self.msg_queue = msg_queue

    def on_data(self, data):
        self.msg_queue.put(data)
        return True

    def on_error(self, status):
        logging.error(status)


class TwitterProducer:
    terms = ['bitcoin', 'usa', 'politics', 'sport', 'soccer']

    def __init__(self):
        self.producer = None
        logging.basicConfig(level=logging.DEBUG)

    def run(self):
        logging.info('Setup')

        """
        Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
        """
        msg_queue = Queue(maxsize=1000)

        # create a twitter client
        client = self.create_twitter_client(msg_queue)

        # create a kafka producer
        self.create_kafka_producer()

        # add a shutdown hook
        atexit.register(exit_handler)

        # loop to send tweets to kafka
        #  on a different thread, or multiple different threads....
        while client.running:
            msg = msg_queue.get(timeout=5)

            if msg:
                logging.info(msg)
                self.producer.produce('twitter_tweets', msg, on_delivery=callback)
        logging.info('End of application')

    def create_twitter_client(self, msg_queue):
        stream = StdOutListener(msg_queue, consumer_key, consumer_secret, token, secret)

        stream.filter(track=self.terms)

        return stream

    def create_kafka_producer(self):
        # create Producer properties
        conf = {'bootstrap.servers': 'localhost:9092'}

        # create the producer
        self.producer = Producer(**conf)


if __name__ == '__main__':
    producer = TwitterProducer()
    producer.run()
