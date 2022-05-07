
from kafka import KafkaConsumer
from datetime import datetime
import psycopg2
import ast
from datetime import datetime
conn = psycopg2.connect("dbname=tweets user=postgres")
import time


cur = conn.cursor()


import sys


bootstrap_servers = ['localhost:9092']

topicNames = ['covid','ipl','football','india','bts']



consumer = KafkaConsumer (topicNames[4], group_id ='group1',bootstrap_servers = 
	  bootstrap_servers)

for msg in consumer:
	final=msg.value.decode() 
	print(msg.topic,final)
	final=final.split('|')
	start_time=final[1].replace(" ","-")
	start_time=start_time.replace(":","-")
	end_time=final[2].replace(" ","-")
	end_time=end_time.replace(":","-")
	s=f"INSERT INTO {msg.topic} (count, start_time, end_time) VALUES ({final[0]},{start_time},{end_time})"
	cur.execute(s)
	conn.commit()
			




sys.exit()