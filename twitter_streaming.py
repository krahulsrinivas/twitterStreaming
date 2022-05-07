import socket
import sys
import requests
import requests_oauthlib
import json
import tweepy
import csv 

api_key="PkSRewLGT4wDSuXxX0sXM88bn"
api_key_secret="KXcX47paYlHZsGNbUq9f4K3DBz4K5v0tszc5b66x6tpDFRV6od"
access_token="1333451942375276545-X2VsUfxxmfptHNH9Bky1xTLRzHrCqz"
access_token_secret="4XHeIx1A1gmUxqlQSDmRRDR8O9UkRlBZxyQP0myLAEHTn"

auth = tweepy.OAuthHandler(api_key,api_key_secret)
auth.set_access_token(access_token,access_token_secret)

api = tweepy.API(auth)

f = open('./tweets.csv', 'w')
writer = csv.writer(f)
fields = ['ID', 'Hashtag', 'Timestamp', 'Text']
writer.writerow(fields) 

def tcp_stream(resp):
	TCP_IP = "localhost"
	TCP_PORT = 8000
	conn = None
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.bind((TCP_IP, TCP_PORT))
	s.listen(1)
	print("Waiting for TCP connection...")
	conn, addr = s.accept()
	print("Connected... Starting getting tweets.")
	# resp = get_tweets()
	send_tweets_to_spark(resp, conn)


class Listener(tweepy.Stream):

	def on_status(self,status):

		if 'hashtags' in status.entities:
			if status.entities['hashtags'] != []:
				for hashtag in status.entities['hashtags']:
					if 'covid' in hashtag['text'].lower():
						print("covid")
						if status.truncated:
							writer.writerow([status.id,"covid",status.extended_tweet['full_text'],status.created_at])
						else:
							writer.writerow([status.id,"covid",status.text,status.created_at])
					elif 'ipl' in hashtag['text'].lower():
						print("ipl")
						if status.truncated:
							writer.writerow([status.id,"ipl",status.extended_tweet['full_text'],status.created_at])
						else:
							writer.writerow([status.id,"ipl",status.text,status.created_at])
					elif 'football' in hashtag['text'].lower():
						print("football")
						if status.truncated:
							writer.writerow([status.id,"football",status.extended_tweet['full_text'],status.created_at])
						else:
							writer.writerow([status.id,"football",status.text,status.created_at])
					elif 'india' in hashtag['text'].lower():
						print("india")
						if status.truncated:
							writer.writerow([status.id,"india",status.extended_tweet['full_text'],status.created_at])
						else:
							writer.writerow([status.id,"india",status.text,status.created_at])
					elif 'bts' in hashtag['text'].lower():
						print("bts")
						if status.truncated:
							writer.writerow([status.id,"bts",status.extended_tweet['full_text'],status.created_at])
						else:
							writer.writerow([status.id,"bts",status.text,status.created_at])


stream_tweet= Listener(api_key,api_key_secret,access_token,access_token_secret)

keywords= ['#covid',"#ipl","#football","#india","#bts"]

stream_tweet.filter(track=keywords)



