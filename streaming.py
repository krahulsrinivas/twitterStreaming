import socket
import sys
import requests
import requests_oauthlib
import json
import tweepy
import csv 





TCP_IP = "localhost"
TCP_PORT = 8000
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")
resp = get_tweets()
send_tweets_to_spark(resp, conn)