SPARK=/pyspark
JARS=$SPARK/jars

spark-submit \
  --jars $JARS/spark-streaming-kafka-assembly_2.11-1.6.3.jar \
  $SPARK/tweet_consumer.py
