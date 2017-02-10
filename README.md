Spark/Kafka/Twitter Stream playground

To get started:
`run bin/spark_setup`

get some twitter creds, copy the `twitter.env.tmpl` to `twitter.env`, and fill in creds

bin/kafka-print-tweets will stream Trump-related tweets (sent from python to kafka)

Still working on how to get the kafka stream into spark (via pyspark)

