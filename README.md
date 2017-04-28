## Spark/Kafka/Twitter Stream playground

### Installation:
  - `run bin/spark_setup`
  - [Create a Twitter app](https://apps.twitter.com/) and get some twitter creds. 
    Then copy the `twitter.env.tmpl` to `twitter.env`, and fill in creds

### Running things:
  - `bin/kafka-print-tweets` will stream Trump-related tweets (sent from python to kafka)
  - `bin/spark-twitter-stream` will spin up the cluster and run `spark/scripts/tweet_consumer.py`

### Notes:
- If your twitter stream is returning 401 errors (and your creds are correct), 
  you may need to restart your docker engine. At least on OSX, the container's system time can drift 
  if the computer goes to sleep, and this violates Twitter's Stream API need for system time to be accurate.

- If adding new supplementary jars in `spark/jars` or adding new requirements to `spark/requirements.txt`,
  you need to rebuild the spark master and worker images, as the build process copies jars/installs python 
  packages in all necessary containers.

