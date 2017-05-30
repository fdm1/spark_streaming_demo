[![Build Status](https://travis-ci.org/fdm1/kafka_spark_streaming.svg?branch=master)](https://travis-ci.org/fdm1/kafka_spark_streaming)

## Spark/Kafka/Twitter Stream playground

### Requirements:
- You need docker and docker-compose installed and running
- [Create a Twitter app](https://apps.twitter.com/) and get some twitter creds.

### Setup:
- Copy the `twitter.env.tmpl` to `twitter.env`, and fill in your twitter creds
- Also set the topic(s) you want to follow in the `twitter.env`

### Running things:
```
# Spin up the environment
$ docker-compose up -d
$ docker-compose exec master bash

# Scripts live in `/spark_scripts/tweetstream` in the docker container.
$ cd /spark_scripts/tweetstream

# Run the TweetStream
$ sbt console
> import tweetstream.TweetStream
> TweetStream.main(new Array(0))

# Build the JAR
$ sbt assembly

# Run the tests
$ sbt test

# Enter the console
$ sbt console

# More to come as the rewrite continues
```

### Notes:
- If your twitter stream is returning 401 errors (and your creds are correct),
  you may need to restart your docker engine. At least on OSX, the container's system time can drift
  if the computer goes to sleep, and this violates Twitter's Stream API need for system time to be accurate.

