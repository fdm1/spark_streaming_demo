## Spark/Kafka/Twitter Stream playground

**Gonna be revisiting/rewriting most/all of this**

### Requirements:
  - You need docker and docker-compose installed and running
  - [Create a Twitter app](https://apps.twitter.com/) and get some twitter creds. 

### Installation:

  - `run bin/spark_setup` - This ensures you have the [gettyimages/docker-spark](https://github.com/gettyimages/docker-spark) 
    repo cloned locally, as docker-compose needs some stuff from here.
  - Copy the `twitter.env.tmpl` to `twitter.env`, and fill in your twitter creds
  - Also set the topic(s) you want to follow in the `twitter.env`

### Running things:

  - `bin/kafka-print-tweets` will stream Trump-related tweets (sent from python to kafka).
  - `bin/spark-twitter-stream [number_of_workers (default=3)]` will spin up the cluster and run `spark/scripts/tweet_consumer.py`


### Notes:
- If your twitter stream is returning 401 errors (and your creds are correct), 
  you may need to restart your docker engine. At least on OSX, the container's system time can drift 
  if the computer goes to sleep, and this violates Twitter's Stream API need for system time to be accurate.

