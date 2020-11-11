from tweepy import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer, KafkaClient
import json
import time
from functools import reduce

API_KEY = ""
API_KEY_SECRET = ""
ACCESS_TOKEN = ""
ACCESS_TOKEN_SECRET = ""
KAFKA_SERVER = "localhost:9092"
TOPIC = "twitter_streaming"
HASHTAGS = ["#election", "#COVID"]
INTERVAL = 5

class TwitterListener(StreamListener):

	def __init__(self, producer, time_limit=60):
		self.limit = time_limit
		self.producer = producer
		self.reset()
		super(TwitterListener, self).__init__()

	def reset(self):
		self.start_time = time.time()

	def on_data(self, data):
		if (time.time() - self.start_time) > self.limit:
			return False

		obj = json.loads(data)
		if "limit" in obj:
			print("rate limited")
			return False

		if len(obj["entities"]["hashtags"]) == 0:
			return True;

		tags = reduce(lambda x, y : x + ";" + y, map(lambda tag: tag["text"], obj["entities"]["hashtags"]))

		if "retweeted_status" in obj:
			try:
				tweet = obj["retweeted_status"]["extended_tweet"]["full_text"]
			except:
				tweet = obj["retweeted_status"]["text"]
		else:
			try:
				tweet = obj["extended_tweet"]["full_text"]
			except:
				tweet = obj["text"]

		self.producer.send(TOPIC, key=tags.encode('utf-8'), value=tweet.encode('utf-8'))
		print(tags + " >>>>\n" + tweet)
		return True

	def on_status(self, status):
		print(status.text)

	def on_error(self, status_code):
		print(f"{status_code=}")
		if status_code == 420:
			time.sleep(INTERVAL)
			return False

def run():
	producer = KafkaProducer(bootstrap_servers=[KAFKA_SERVER])
	auth = OAuthHandler(API_KEY, API_KEY_SECRET)
	auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
	listener = TwitterListener(producer, INTERVAL)

	while True:
		listener.reset()
		stream = Stream(auth, listener)
		stream.filter(languages=["en"], track=HASHTAGS, is_async=True)
		time.sleep(INTERVAL)

if __name__ == "__main__":
    run()


