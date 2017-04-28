package com.frankmassi.twitter_analyzer

import com.google.common.collect.ImmutableMap
import com.google.gson.Gson
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{desc, explode}
import twitter4j.Status

/**
 * Collect at least the specified number of tweets into json text files.
 */

object rddFunctions {
  private var tweetSeparatorString = "TWEETSPLITTER"

  def getRawTweetText(status: twitter4j.Status): String = {
    if (status.isRetweet) { status.getRetweetedStatus().getText() + tweetSeparatorString + "RETWEET" }
    else { status.getText() }
  }

  def getCleanedStatusText(text: String): String = {
    val cleaner_regex = "(^rt |(^| )@\\S*|http\\S*|[^a-z0-9 #])".r
    val raw_tuple = text.split(tweetSeparatorString)
    val clean_text = raw_tuple(0) + tweetSeparatorString + cleaner_regex.replaceAllIn(raw_tuple(0).toLowerCase, "").trim
    if (raw_tuple.length == 2) { clean_text + tweetSeparatorString + raw_tuple(1) }
    else { clean_text }
  }

  def getPipedFlatMap(rdd: RDD[String]): RDD[String] = {
    rdd.pipe("python ./pipe_scripts/wordlist_flatmap.py " + tweetSeparatorString)
  }
}

case class TweetData(
  uuid: String,
  raw_text: String,
  words: Array[String],
  hashtags: Array[String],
  is_retweet: String
)

case class TweetWordHash(
  uuid: String,
  word: String,
  hashtag: String="",
  is_retweet: String
)


/** Lazily instantiated singleton instance of SparkSession */
object SparkSessionSingleton {

  @transient  private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}

object Collect {
  private var numTweetsToCollect = 60000L
  private var numTweetsCollected = 0L
  private var gson = new Gson()
  private var intervalSecs = 5
  private var filter_words = Array(sys.env("tweet_topics"))

  def main(args: Array[String]) {

    println("Initializing Streaming Spark Context...")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(intervalSecs))
    val tweetStream = TwitterUtils.createStream(ssc, Utils.getAuth, filter_words)
    val rawStatusText = tweetStream.map(rddFunctions.getRawTweetText)
    val tweetWords = rawStatusText.map(rddFunctions.getCleanedStatusText)
    val pipedWords = tweetWords.transform(rddFunctions.getPipedFlatMap(_))
    val tweetDataRDD = pipedWords.map(piped_json => gson.fromJson(piped_json, classOf[TweetWordHash]))

    val windowedRDD = tweetDataRDD.window(Seconds(900), Seconds(15))
    // windowedRDD.print
    // tweetDataRDD.map(r => (r.hashtag, 1))
    //             .reduceByKeyAndWindow(_ + _, Seconds(900))
    //             .map { case (topic, count) => (count, topic)}
    //             .transform(_.sortByKey(false)).print
    //
    val wordCounts = (windowedRDD.map(r => (r.word, 1))
                                 .reduceByKey(_ + _))

    val topWords = (wordCounts.map { case (topic, count) => (count, topic) }
                              .transform(_.sortByKey(false))
                              .map { case (topic, count) => (count, topic) })

    val wordHashCounts = (windowedRDD.map(r => ((r.word, r.hashtag, r.is_retweet), 1))
                                      .reduceByKey(_ + _)
                                      .map(r => (r._1._1, (r._1._2, r._1._3, r._2))))

    val topWordHashCounts = topWords.join(wordHashCounts)
                                    .map(r => ((r._1, r._2._1), r._2._2))

    case class TotalCount(
      word: String,
      total_count: Int
    )

    case class HashRetweetCount(
      hashtag: String,
      is_retweet: Boolean,
      count: Int
    )

    val structuredCounts = (topWordHashCounts.map(r => ((r._1._1, r._1._2),
                                                        (r._2._1, r._2._2 == "true", r._2._3))))


    case class TopWordsByHashtag(
      word: String,
      totalCount: Int,
      hashtags: Array[(String, Boolean, Int)]
    )

    val topWordsByHashAndRetweeet = structuredCounts.groupByKey
                                                    .map(r => (r._1._1, r._1._2, r._2.toArray))
                                                    .map(r => gson.toJson(r))
                                                    .print
    // gs.toJson(topWordsByHashAndRetweeet.collect)

    rawStatusText.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        numTweetsCollected += count
        println(s"Total tweet counts: $numTweetsCollected (limit $numTweetsToCollect)")
        if (numTweetsCollected > numTweetsToCollect) {
          System.exit(0)
        }
      }
    })

		ssc.checkpoint("/tmp/checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}
    // windowedRDD.map(r => gson.toJson(r)).print
    // windowedRDD.transform(r => {
    //   for (w <- r.words) {
    //     for (h <- r.hashtags) {
    //       val uuid = r.uuid
    //       val is_retweet = r.is_retweet
    //       println(s"uuid=$uuid, word=$w, hashtag=$h, is_retweet=$is_retweet")
    //       (uuid, w, h, is_retweet)
    //     }
    //   }
    // }).print
    // windowedRDD.foreachRDD { (rdd, time) =>
    //   // Get the singleton instance of SparkSession
    //   val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
    //   import spark.implicits._
    //
    //   val df = rdd.toDF
    //               .withColumn("words", explode($"words")).withColumnRenamed("words", "word")
    //               .withColumn("hashtags", explode($"hashtags")).withColumnRenamed("hashtags", "hashtag")
    //
    //   val counts = df.groupBy($"word").count
      // df.createOrReplaceTempView("tweets")
      // spark.sqlContext.sql("select word, count(1) from tweets group by 1 order by 2 desc limit 10").show
    // }


//
//
// import com.google.common.collect.ImmutableMap
// import com.google.gson.Gson
//
// val gson = new Gson()
//
// case class TweetData(uuid: String,
//                      raw_text: String,
//                      words: Array[String],
//                      // polarity: Double,
//                      hashtags: Array[String],
//                      is_retweet: String
//                     )
//
// val t1 = TweetData("a", "hi you", Array("hi", "you"), Array(""), "false")
// val t2 = TweetData("b", "hi there #yo", Array("hi", "there"), Array("#yo", "#you"), "true")
// val t3 = TweetData("c", "hey you #you", Array("hey", "you"), Array("#you"), "true")
// val t4 = TweetData("d", "hi guy hey #you", Array("hi", "guy", "hey"), Array("#you"), "false")
// val t = List(t1, t2, t3, t4)
//
//
//
// val tweets = sc.parallelize(t)

// case class TweetWordHash(
//   uuid: String,
//   word: String,
//   hashtag: String="",
//   is_retweet: String
// )
//
// val data = List(TweetWordHash("a", "hi", "#hi", "false"),
//                 TweetWordHash("a", "there", "#hi", "false"),
//                 TweetWordHash("b", "hi", "#hi", "false"),
//                 TweetWordHash("b", "hi", "#ho", "false"),
//                 TweetWordHash("c", "yo", "", "true"))
// val tweetDataRDD = sc.parallelize(data)
//
// val wordCounts = (tweetDataRDD.map(r => (r.word, 1))
//                              .reduceByKey(_ + _))
//
// val topWords = sc.parallelize(wordCounts.sortBy(_._2, false).take(2))
//
// val wordHashCounts = (tweetDataRDD.map(r => ((r.word, r.hashtag, r.is_retweet), 1))
//                                  .reduceByKey(_ + _)
//                                  .map(r => (r._1._1, (r._1._2, r._1._3, r._2))))
// wordHashCounts
// val topWordHashCounts = topWords.join(wordHashCounts).map(r => ((r._1, r._2._1), r._2._2))
//
// case class TotalCount(
//   word: String,
//   total_count: Int
// )
//
// case class HashRetweetCount(
//   hashtag: String,
//   is_retweet: Boolean,
//   count: Int
// )
//
// val structuredCounts = topWordHashCounts.map(r => (TotalCount(r._1._1, r._1._2), HashRetweetCount(r._2._1, r._2._2 == "true", r._2._3)))
//
//
// case class TopWordsByHashtag(
//   word: String,
//   totalCount: Int,
//   hashtags: Array[HashRetweetCount]
// )
// val topWordsByHashAndRetweeet = structuredCounts.groupByKey.map(r => TopWordsByHashtag(r._1.word, r._1.total_count, r._2.toArray))
// gs.toJson(topWordsByHashAndRetweeet.collect)
//
// val df = (tweets.toDF
//               .withColumn("words", explode($"words")).withColumnRenamed("words", "word")
//               .withColumn("hashtags", explode($"hashtags")).withColumnRenamed("hashtags", "hashtag"))
// df.createOrReplaceTempView("tweets")
// val sql = spark.sqlContext
// sql.sql("select word, count(1) from tweets group by 1 order by 2 desc limit 10").show
// val all_rollups = df.rollup("word", "hashtag").count
// all_rollups.createOrReplaceTempView("all_rollup")
// val all_rollup_counts = sql.sql("select * from all_rollup where word != 'null' and hashtag != 'null' order by word asc, count desc, hashtag desc")
// all_rollup_counts.show
//
// val orig_rollups = df.filter(!$"retweet").rollup("word", "hashtag").count
// orig_rollups.createOrReplaceTempView("orig_rollup")
// val orig_rollup_counts = sql.sql("select * from orig_rollup where word != 'null' and hashtag != 'null' order by word asc, count desc, hashtag desc")
// orig_rollup_counts.show
//
