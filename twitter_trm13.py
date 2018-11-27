import json
import os
import pandas as pd
import pyarrow as pa
# import pyarrow.parquet
import sys
import tweepy
# TRM
from tweepy import Stream
import pyarrow.parquet as pq
import time

# Output directory
output_directory = "tweets"
t0 = time.time()


# Models a single tweet
class Tweet:
    def __init__(self, json_map):

        if 'extended_tweet' in json_map:
            self.text = str(json_map['extended_tweet']['full_text'])

        else:
            self.text = str(json_map['text'])

        self.id = str(json_map['id_str'])
        self.username = json_map['user']['screen_name']
        self.date = str(json_map['created_at'])
        self.retweet = str(json_map['retweeted'])
        self.lang = str(json_map['lang'])
        self.geo = str(json_map['place'])
        self.brand = argm_check()



# This is a basic listener that batches tweets and stores them in Parquet.
class TweetListener(tweepy.streaming.StreamListener):

    def __init__(self, nbatchs = 100, batch_size = 5, brand = None ):
        super().__init__()
        self.tweets_brand = [] #brand
        self.tweets_id = []  # id
        self.tweets_user = []  # user
        self.tweets_date = []  # created_at
        self.tweets_retweet = [] #retweet
        self.tweets_language = []  # lang
        self.tweets_location = []  # place
        self.tweets_buffer = [] #text
        #
        self.counter = 0
        self.batch = 0
        #
        #Arguments
        self.access_token = sys.argv[1]
        self.access_token_secret = sys.argv[2]
        self.consumer_key = sys.argv[3]
        self.consumer_secret = sys.argv[4]
        self.brand = argm_check()
        if len(sys.argv) == 8:
            self.batch_size = int(sys.argv[6])
            self.nbatchs = int(sys.argv[7])
        else:
            self.batch_size = 10
            self.nbatchs = 3
        #
        self.auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        self.auth.set_access_token(self.access_token, self.access_token_secret)
        self.stream = Stream(self.auth, self)
        self.api = tweepy.API(self.auth)

        if self.brand is not None:
            self.stream.filter(languages=['en'], track=[self.brand])
        else:
            self.stream.sample(languages=['en'])

        self.ind_tweet = []
        self.on_data = []
        self.tweet = []

        #

    def ind_tweet(self, data):
        json_map = json.loads(data)
        count = 0
        if 'id_str' not in json_map:
            self.tweet.id = 'N/A'
            count += 1
        if 'user' not in json_map:
            self.tweet.username = 'N/A'
            count += 1
        if 'created_at' not in json_map:
            self.tweet.date = 'N/A'
            count += 1
        if 'retweeted' not in json_map:
            self.tweet.retweet = 'N/A'
            count += 1
        if 'lang' not in json_map:
            self.tweet.lang = 'N/A'
            count += 1
        if 'place' not in json_map:
            self.tweet.geo = 'N/A'
            count += 1
        if 'text' not in json_map:
            self.tweet.text = 'N/A'
            count += 1
        if count >= 1:
            return self.tweet
        else:
            self.tweet = Tweet(json_map)
            return self.tweet



    def on_data(self, data):

        self.tweet = self.ind_tweet(data)

        if ('RT @' not in self.tweet.text):

            print(self.counter, self.tweet.text, self.tweet.date, self.tweet.username)

            #self.tweets_brand.append(str(self.brand))
            self.tweets_id.append(str(self.tweet.id))
            self.tweets_user.append(str(self.tweet.username))
            self.tweets_date.append(str(self.tweet.date))
            self.tweets_retweet.append(str(self.tweet.retweet))
            self.tweets_language.append(str(self.tweet.lang))
            self.tweets_location.append(str(self.tweet.geo))
            self.tweets_buffer.append(str(self.tweet.text))

            self.counter += 1

            # Save DF
            if len(self.tweets_buffer) >= self.batch_size:
                for each in self.tweets_buffer:
                    #tweets = pd.DataFrame(self.tweets_buffer, columns=['text'])
                    tweets = pd.DataFrame()
                    #
                    #tweets['brand'] = self.tweets_brand
                    tweets['id'] = self.tweets_id
                    tweets['user'] = self.tweets_user
                    tweets['date'] = self.tweets_date
                    tweets['retweet'] = self.tweets_retweet
                    tweets['language'] = self.tweets_language
                    tweets['country'] = self.tweets_location
                    tweets['text'] = self.tweets_buffer
                    #
                    table = pa.Table.from_pandas(tweets)
                    dir = str(output_directory + 'tweet_{0}_run{1}.parquet'.format(self.brand, self.batch))
                    pqdf = pq.write_table(table, 'tweets/tweet_{0}_run{1}.parquet'.format(self.brand, self.batch))


                self.tweets_id = []  # id
                self.tweets_user = []  # user
                self.tweets_date = []  # created_at
                self.tweets_retweet = []  # retweet
                self.tweets_language = []  # lang
                self.tweets_location = []  # geo
                self.tweets_buffer = [] #text

                self.batch += 1

            if self.batch >= self.nbatchs:
                self.stream.disconnect()
                return pqdf

    def on_error(self, status):
        print('Something went wrong. Status', status)


def get_instance_data(brand=None):
    data = TweetListener(1)


def argm_check():
    # Parse arguments that contains the user credentials to access Twitter API
    if len(sys.argv) < 5 or len(sys.argv) > 8:
        print('Usage: <access_token> <access_token_secret> <consumer_key> <consumer_secret> [brand]')
        sys.exit(1)

    if len(sys.argv) == 6 or len(sys.argv) == 8:
        brand = sys.argv[5]
    else:
        brand = None

    return brand


def main():
    brand = argm_check()

    # Create output directory if it does not yet exist
    if not os.path.isdir(output_directory):
        os.makedirs(output_directory)

    # TRM
    access_token = sys.argv[1]
    access_token_secret = sys.argv[2]
    consumer_key = sys.argv[3]
    consumer_secret = sys.argv[4]
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)


    data = TweetListener(1, brand)


if __name__ == '__main__':
    main()
