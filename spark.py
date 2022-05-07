import numpy as np
import os.path
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.types import *
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
from pyspark.sql.functions import window
from kafka import KafkaProducer

bootstrap_servers = ['localhost:9092']


producer = KafkaProducer(bootstrap_servers = bootstrap_servers)



sc = SparkContext("local[2]", "DBT")
ssc = StreamingContext(sc, 1)
spark = SparkSession.builder.appName("DBT").getOrCreate()

schema = StructType([
        StructField("ID", IntegerType(), True),
        StructField("Hashtag", StringType(), True),
        StructField("Text", StringType(), True),
        StructField("Timestamp", TimestampType(), True),
      ])

df=spark.readStream.schema(schema).option("multiline","true").json('/Users/rahulsrinivas/Desktop/dbtproject/tweets*.json')

# df.head()
# df.show()

tumblingaggregations = df.withWatermark("timestamp", "5 minutes").groupBy(window("Timestamp", "5 minutes"),"Hashtag").count()


tumblingaggregations.writeStream.outputMode("complete").format("console").start()

# query=tumblingaggregations.writeStream.outputMode("append").format("json").option("maxRecordsPerFile", 20).option("truncate", False).option("path","/Users/rahulsrinivas/Desktop/dbtproject/counts.json").option("checkpointLocation", "/Users/rahulsrinivas/Desktop/dbtproject/counts.json").start()

def process_row(df,epoch_id):
    row= df.head()
    if row:
        for i in df.collect():
            # print(i["Hashtag"])
            message=str(i['count'])+"|"+str(i['window']['start'])+"|"+str(i['window']['end'])
            message=str(message).encode()
            producer.send(i["Hashtag"], message)
        producer.flush()
        producer.close()



query = tumblingaggregations.writeStream.foreachBatch(process_row).start()

query.awaitTermination()

# lines = ssc.socketTextStream("localhost", 8000)
# lines.pprint()





    


#
# def batch(rdd):
#     df=spark.read.json(rdd)
#     row= df.head()
#     if row:
#         print(df.fields)
#     # if row:

    
# lines.foreachRDD(lambda rdd: batch(rdd))

# ssc.start()             # Start the computation
# ssc.awaitTermination()