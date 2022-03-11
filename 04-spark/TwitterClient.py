import configparser
import socket
import sys
import requests
import requests_oauthlib
import json

config = configparser.ConfigParser()
config.read('twitter.env')
consumer_key = config.get('Twitter', 'TWITTER_API_KEY')
consumer_secret = config.get('Twitter', 'TWITTER_SECRET_KEY')
token = config.get('Twitter', 'TWITTER_TOKEN')
secret = config.get('Twitter', 'TWITTER_TOKEN_SECRET')
my_auth = requests_oauthlib.OAuth1(consumer_key, consumer_secret, token, secret)

def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        full_tweet = json.loads(line)
        tweet_text = full_tweet['text'] + '\n' # pyspark can't accept stream, add '\n'
        print("Tweet Text: " + tweet_text)
        print ("------------------------------------------")
        tcp_connection.send(tweet_text.encode("utf-8"))


def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('locations', '-122.75,36.8,-121.75,37.8,-74,40,-73,41'), ('track', '#')] #this location value is San Francisco & NYC
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response


TCP_IP = "localhost"
TCP_PORT = 9009
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
resp = get_tweets()
send_tweets_to_spark(resp,conn)