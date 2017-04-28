SPARK=/pyspark
JARS=$SPARK/jars

cd $SPARK

pyspark \
  --jars \
  $JARS/spark-streaming-kafka-assembly_2.11-1.6.3.jar
