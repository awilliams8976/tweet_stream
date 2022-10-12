import constants
import csv
import os
import time
import tweepy
from dotenv import load_dotenv

load_dotenv()

access_token = os.environ.get("TWITTER_ACCESS_TOKEN")
access_token_secret = os.environ.get("TWITTER_ACCESS_TOKEN_SECRET")
api_key = os.environ.get("TWITTER_API_KEY")
api_secret = os.environ.get("TWITTER_API_SECRET")
bearer_token = fr'{os.environ.get("TWITTER_BEARER_TOKEN")}'

topics = ["fantasy football"]

class Stream(tweepy.StreamingClient):

    def __init__(self, output_file, bearer_token, *, return_type=tweepy.Response,wait_on_rate_limit=False, **kwargs):
        self.output_file = output_file
        tweepy.StreamingClient.__init__(self, bearer_token, return_type=tweepy.Response,wait_on_rate_limit=False, **kwargs)
        
    def on_connect(self):
        print("Connected")

    def on_tweet(self, tweet):
        print(f"Writing Tweet {tweet.id} data to {self.output_file}")

        with open(self.output_file,'a',encoding='UTF8',newline='') as f:
            writer = csv.writer(f,delimiter="|")
            writer.writerow(
                [
                    tweet.id,
                    tweet.created_at,
                    tweet.text.replace("\n","\\n").replace("\r","\\r"),
                    tweet.entities,
                    tweet.geo,
                    tweet.lang,
                    tweet.public_metrics,
                    tweet.referenced_tweets
                ]
            )

        time.sleep(0.5)

stream = Stream(output_file="./output_file.txt",bearer_token=bearer_token)
for topic in topics:
    stream.add_rules(tweepy.StreamRule(topic))
stream.filter(tweet_fields=["id","text", "created_at","entities", "geo", "lang", "public_metrics","referenced_tweets"])