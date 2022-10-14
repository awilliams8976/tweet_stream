import constants
import csv
import json
import os
import time
from dotenv import load_dotenv
from mysql.connector import connect, cursor
from tweepy import StreamingClient, StreamRule
from tweepy.tweet import Tweet
from collections import namedtuple

load_dotenv()

access_token = os.environ.get("TWITTER_ACCESS_TOKEN")
access_token_secret = os.environ.get("TWITTER_ACCESS_TOKEN_SECRET")
api_key = os.environ.get("TWITTER_API_KEY")
api_secret = os.environ.get("TWITTER_API_SECRET")
bearer_token = fr'{os.environ.get("TWITTER_BEARER_TOKEN")}'
mysql_host = os.environ.get("MYSQL_HOST")
mysql_password = os.environ.get("MYSQL_PASSWORD")
mysql_username = os.environ.get("MYSQL_USERNAME")

topics = ["fantasy football"]

StreamResponse = namedtuple("StreamResponse", ("data", "includes", "errors", "matching_rules"))

class LogTweetsToDb(StreamingClient):

    def __init__(self, bearer_token, host, user, password, output_file):
        StreamingClient.__init__(self, bearer_token=bearer_token)
        self.db_conn = connect(host=host, user=user, password=password)
        self.db_cursor = self.db_conn.cursor()
        # self.output_file = output_file

    def on_connect(self):
        print("Connected")

    def on_data(self, raw_data):
        
        data = json.loads(raw_data)

        # tweet = None
        # includes = {}
        # errors = []
        # matching_rules = []

        # if "data" in data:
        #     tweet = Tweet(data["data"])
        #     self.on_tweet(tweet)
        # if "includes" in data:
        #     includes = self._process_includes(data["includes"])
        #     self.on_includes(includes)
        # if "errors" in data:
        #     errors = data["errors"]
        #     self.on_errors(errors)
        # if "matching_rules" in data:
        #     matching_rules = [
        #         StreamRule(id=rule["id"], tag=rule["tag"])
        #         for rule in data["matching_rules"]
        #     ]
        #     self.on_matching_rules(matching_rules)

        # print(StreamResponse(tweet, includes, errors, matching_rules))
        print(data)
    # def on_tweet(self, tweet):
    #     print(f"""Writing Tweet data {tweet.id} to db.""")
        
    #     self.db_cursor.execute(
    #         """
    #             INSERT INTO data_eng.tweets_stage (
    #                 tweetId,
    #                 tweetCreatedAt,
    #                 tweetAuthorId,
    #                 tweetText,
    #                 tweetConversationId,
    #                 tweetInReplyToUserId,
    #                 tweetEntities,
    #                 tweetGeo,
    #                 tweetLang,
    #                 tweetPublicMetrics,
    #                 tweetReferencedTweets
    #             )

    #             VALUES 
    #             (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    #         """,
    #         (
    #             tweet.id,
    #             tweet.created_at,
    #             tweet.author_id,
    #             tweet.text,
    #             tweet.conversation_id,
    #             tweet.in_reply_to_user_id,
    #             str(tweet.entities),
    #             str(tweet.geo),
    #             tweet.lang,
    #             str(tweet.public_metrics),
    #             str(tweet.referenced_tweets)
    #         )
    #     )

    #     self.db_conn.commit()

    #     time.sleep(0.5)

stream = LogTweetsToDb(bearer_token=bearer_token, host=mysql_host, user=mysql_username, password=mysql_password, output_file="./output_file.txt")
# stream.get_rules()
# stream = StreamingClient(bearer_token=bearer_token)
# stream.delete_rules(stream.get_rules())
for topic in topics:
    stream.add_rules(StreamRule(topic))
stream.filter(tweet_fields=["id","created_at","author_id","text","conversation_id","in_reply_to_user_id","entities","geo","lang","public_metrics","referenced_tweets"],user_fields=["username","url"])
# print(stream.on_data())
