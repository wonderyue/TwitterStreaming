from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from elasticsearch import Elasticsearch
import json


TOPIC = "twitter_streaming"
BROKERS = "localhost:9092"
CHECKPOINT = "twitter_checkpoint"
WINDOW_SIZE = 5
ES_INDEX = "labeled_tweets"
ELASTICSEARCH_HOST = "localhost"
ELASTICSEARCH_PORT = "9200"


nltk.download('vader_lexicon')

def get_sentiment_label(text, analyzer):
	labels = {'neg': 0, 'neu' : 0, 'pos': 0}
	res = analyzer.polarity_scores(text)
	label = None;
	max_score = 0;
	for l in labels:
		if res[l] > max_score:
			max_score = res[l]
			label = l
	labels[label] = 1
	return labels

def aggregate(x, y):
	return {'neg': x['neg'] + y['neg'], 'neu' : x['neu'] + y['neu'], 'pos': x['pos'] + y['pos']}

def aggregate_sentiment_by_hashtag(new_values, old_value):
	res = {'neg': 0, 'neu' : 0, 'pos': 0}
	for v in new_values:
		res = aggregate(res, v)
	return res if old_value is None else aggregate(res, old_value)

def run():
	# init spark
	conf = SparkConf().set("spark.io.compression.codec", "org.apache.spark.io.SnappyCompressionCodec")
	sc = SparkContext(appName="TwitterStreaming", conf=conf)
	ssc = StreamingContext(sc, WINDOW_SIZE)
	ssc.checkpoint(CHECKPOINT)
	# ssc = functionToCreateContext()
	# init kafka
	ks = KafkaUtils.createDirectStream(ssc, [TOPIC], {"metadata.broker.list": BROKERS})
	# init nltk
	analyzer = SentimentIntensityAnalyzer()
	# init elasticsearch
	es = Elasticsearch()
	# igrone IndexAlreadyExistsException
	es.indices.create(index=ES_INDEX, ignore=400) 
	# hashtags_2_label : key: "tag1;tag2;tag3", value: {'neg': n, 'neu' : n, 'pos': n}
	hashtags_2_label = ks.map(lambda kv : (kv[0], get_sentiment_label(kv[1], analyzer)))
	# tag_2_label : key: "tag", value: {'neg': n, 'neu' : n, 'pos': n}
	tag_2_label = hashtags_2_label.flatMap(lambda kv: map(lambda k: (k, kv[1]), kv[0].split(";")))
	tag_2_label = tag_2_label.updateStateByKey(aggregate_sentiment_by_hashtag)
	# send to elasticsearch
	es_write_conf = {
		"es.nodes": ELASTICSEARCH_HOST,
		"es.port": ELASTICSEARCH_PORT,
		"es.resource": ES_INDEX,
		"es.input.json": "yes",
		"es.mapping.id":"id"
	}
	tag_2_label = tag_2_label.map(lambda kv: (None, json.dumps({"id":kv[0], "data":kv[1]})))
	tag_2_label.foreachRDD(lambda line : line.saveAsNewAPIHadoopFile(
		path="tweets", 
		outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
		keyClass="org.apache.hadoop.io.NullWritable", 
		valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
		conf=es_write_conf))


	ssc.start()
	ssc.awaitTermination()


if __name__ == "__main__":
	run()
