[Spark Streaming Udemy Course](https://pplearn.udemy.com/taming-big-data-with-spark-streaming-hands-on/learn/v4/overview)

run twitter stream:

  - copy in creds
  - `cd SparkScala && mvn install`
  - ```
    docker-compose run --rm master spark-submit \
      --jars /course_scripts/SparkScala/jars/spark-core_2.11-1.5.2.logging.jar \
      --class com.frankmassi.sparkstreaming.PrintTweets /course_scripts/SparkScala/target/SparkScala-0.1.jar
    ```


