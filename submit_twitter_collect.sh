spark-submit \
     --class "com.databricks.apps.twitter_classifier.Collect" \
     --master local[4] \
     target/scala-2.11/SparkStreaming1-assembly-1.0.jar \
     ${YOUR_OUTPUT_DIR:-/tmp/tweets} \
     ${NUM_TWEETS_TO_COLLECT:-10000} \
     ${OUTPUT_FILE_INTERVAL_IN_SECS:-10} \
     ${OUTPUT_FILE_PARTITIONS_EACH_INTERVAL:-1} \
