DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SCALA_DIR=/SparkScala

mvn install -f $DIR/$SCALA_DIR/pom.xml
docker-compose run --rm master spark-submit \
  --jars $SCALA_DIR/jars/spark-core_2.11-1.5.2.logging.jar \
  --class com.frankmassi.sparkstreaming.$1 $SCALA_DIR/target/SparkScala-0.1.jar

