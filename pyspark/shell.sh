SPARK=/pyspark
JARS=$SPARK/jars


pyspark \
  --jars \
$JARS/spark-core_2.11-1.5.2.logging.jar,\
$JARS/spark-streaming-kafka-assembly_2.10-1.6.3.jar
	# $SPARK/tweet_consumer.py
