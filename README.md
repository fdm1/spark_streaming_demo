## Spark/Kafka/Twitter Stream playground

### Requirements:
  - You need docker and docker-compose installed and running
  - [Create a Twitter app](https://apps.twitter.com/) and get some twitter creds. 

### Installation:
  - `run bin/spark_setup` - This ensures you have a clone of the [gettyimages/docker-spark](https://github.com/gettyimages/docker-spark) repo in, as docker-compose needs some stuff from here.
  - Copy the `twitter.env.tmpl` to `twitter.env`, and fill in your twitter creds

### Running things:
  - `bin/kafka-print-tweets` will stream Trump-related tweets (sent from python to kafka). Note that this sends the full [Tweet object](https://dev.twitter.com/overview/api/tweets), so this will be noisy.
  - `bin/spark-twitter-stream` will spin up the cluster and run `spark/scripts/tweet_consumer.py`

### Notes:
- If your twitter stream is returning 401 errors (and your creds are correct), 
  you may need to restart your docker engine. At least on OSX, the container's system time can drift 
  if the computer goes to sleep, and this violates Twitter's Stream API need for system time to be accurate.

- If adding new supplementary jars in `spark/jars` or adding new requirements to `spark/requirements.txt`,
  you need to rebuild the spark master and worker images, as the build process copies jars/installs python 
  packages in all necessary containers.

