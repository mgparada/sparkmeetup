import tweepy
import config
import json
import array
import os

class StdOutListener(tweepy.StreamListener):
	
	def on_data(self, data):
		global tweets, counter		
		tweet = json.loads(data)
		hashtags = tweet['entities']['hashtags']
		if (len(hashtags)>=2 and len(hashtags)<=4):		
			print '------------------------------------------'
			newtweet=[]
			for hashtag in hashtags:
				print hashtag['text']
				newtweet.append(hashtag['text'])	
			tweets.append(newtweet)
			counter= counter-1		
			if (counter==0):
				stopStream()

	#whenever an error occurs
	def on_error(self, status):
		print status

def startStream():
	global stream
	print "gathering tweets"
	#filter by two hastags and only spanish and english tweets
	stream.filter(track=['#madrid,#barcelona'],languages=["en","es"], async=True)


def stopStream():
	global stream
	fout=open('tweets.json','w')	
	for hashtags in tweets:
		fout.write(json.JSONEncoder().encode({"hashtags":hashtags})+'\n')
	fout.close()
	stream.disconnect()

def main():
	startStream()

if __name__ == '__main__':
	auth=tweepy.OAuthHandler(config.consumer_key, config.consumer_secret)
	auth.set_access_token(config.access_token,config.access_token_secret)
	listener= StdOutListener()
	stream=tweepy.Stream(auth,listener)
	tweets=[]
	counter=10
	main()

