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

