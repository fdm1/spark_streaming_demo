package com.frankmassi.twitter_analyzer

import com.google.gson.Gson
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import twitter4j.Status

/**
 * Collect at least the specified number of tweets into json text files.
 */

// val pipe_separator = "TWEETSPLITTER"

object rddFunctions {
  private var tweetSeparatorString = "TWEETSPLITTER"

  def getRawTweetText(status: twitter4j.Status): String = {
    if (status.isRetweet) { status.getRetweetedStatus().getText() }
    else { status.getText() }
  }

  def getCleanedStatusText(text: String): String = {
    val cleaner_regex = "(^rt |(^| )@\\S*|http\\S*|[^a-z0-9 #])".r
    text + tweetSeparatorString + cleaner_regex.replaceAllIn(text.toLowerCase, "").trim
  }

  def getPipedSentiment(rdd: RDD[String]): RDD[String] = {
    rdd.pipe("python ./pipe_scripts/wordlist_sentiment.py " + tweetSeparatorString)
  }
  

}

case class TweetData(raw_text: String,
                     words: Array[String],
                     hashtags: Array[String],
                     polarity: Float)

object Collect {
  private var numTweetsToCollect = 1000L
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

    // val tweetStream = TwitterUtils.createStream(ssc, Utils.getAuth).map(gson.toJson(_))
    val tweetStream = TwitterUtils.createStream(ssc, Utils.getAuth, filter_words)
    
    val rawStatusText = tweetStream.map(rddFunctions.getRawTweetText)

    val tweetWords = rawStatusText
                        .map(rddFunctions.getCleanedStatusText)
                        .transform(rddFunctions.getPipedSentiment(_))
                        .map(piped_json => gson.fromJson(piped_json, classOf[TweetData]))
                        //
                        // .flatMap(tweetText => tweetText.split(" ").toSet)
                        // .map(word => word.trim)
                        // .filter(word => word.length > 1)
    
    val fm = tweetWords.flatMap(td => {
      for (x <- td.words; y <- td.hashtags; z <- List(td.polarity)) yield (x,y,z)
    })

    val top_words = fm.transform(rdd => {
      rdd.zipWithUniqueId
         .groupBy(r => r._1._1)
         .map(r => (r._1, r._2.count(x => true)))
         .sortBy(r => r._2, ascending=false)
    })

    
    val win = top_words.window(Seconds(300), Seconds(5))
    win.print
    // get top words
    // val topWordCounts = tweetWords
    //                       .filter(word => !word.startsWith("#"))
    //                       .transform(rdd => rdd.pipe("./python/stopwords_filter.py"))
    //                       .map(word => (word, 1))
    //                       .reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(5))
    //                       .transform(rdd => rdd.sortBy(x => x._2, false))
    // topWordCounts.print
    //
    // // get top hashtags
    // val topHashtagCounts = tweetWords
    //                         .filter(word => word.startsWith("#"))
    //                         .map(word => (word, 1))
    //                         .reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(5))
    //                         .transform(rdd => rdd.sortBy(x => x._2, false))
    // topHashtagCounts.print
    //
    // def getCleanedStatus(text: String): String = {
    //   val cleaner_regex = "(^rt |(^| )@\\S*|http\\S*|[^a-z0-9 #])".r
    //   text.map(status => cleaner_regex.replaceAllIn(status.toLowerCase, "").trim)
    // }
    //
    // def getHashtags(text: String): List[String] = {
    //   text.split(" ").filter(word => word.startsWith("#")).toSet
    // }
    //
    // def getWords(text: String): List[String] = {
    //   text.split(" ").filter(word => !word.startsWith("#")).toSet
    // }
    //
    // def getRetweet(status: Status): Boolean = status.isRetweet



    // rawStatusText.print

    // clean text (remove dupe spaces, RT, urls, mentions, non-alphanumeric, then lowercase), flatmap to words
    // val extractedTweets = tweetStream.map(tweet =>
    // val cleaner_regex = "(^rt |(^| )@\\S*|http\\S*|[^a-z0-9 #])".r
     

    // OLD WORDS
    // val words = tweetWords.filter(word => !word.startsWith("#"))
    // val wordKeyValues = words.map(word => (word, 1))
    // val wordCounts = wordKeyValues.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(5))
    // val sortedWordResults = wordCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    // sortedWordResults.print

    // OLD HASHTAG
    // get top hashtags
    // Now eliminate anything that's not a hashtag
    // val hashtags = tweetWords.filter(word => word.startsWith("#"))
    // // Map each hashtag to a key/value pair of (hashtag, 1) so we can count them up by adding up the values
    // val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))
    // // Now count them up over a 5 minute window sliding every one second
    // val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(5))
    // //  You will often see this written in the following shorthand:
    // //val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( _ + _, _ -_, Seconds(300), Seconds(1))
    // // Sort the results by the count values
    // val sortedHashResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    // // Print the top 10
    // sortedHashResults.print

		// Stop after X tweets
    rawStatusText.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        numTweetsCollected += count
        println(s"Total tweet counts: $numTweetsCollected (limit $numTweetsToCollect)")
        if (numTweetsCollected > numTweetsToCollect) {
          System.exit(0)
          // ssc.stop()
        }
      }
    })

		ssc.checkpoint("/tmp/checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}



// object Scratch {
//
//   case class TweetData(raw_text: String,
//                        words: Array[String],
//                        hashtags: Array[String],
//                        polarity: Double,
//                        other_num: Double)
//
//   val t1 = TweetData("hi there", Array("hi", "there"), Array(), 0.3, 2)
//   val t2 = TweetData("hi there #yo", Array("hi", "there"), Array("#yo"), 0.0, 1)
//   val t3 = TweetData("hey #you", Array("hey"), Array("#you"), -0.3, 2)
//   val t = List(t1, t2, t3)
//   val tweets = sc.parallelize(t)
//
//   val fm = tweets.flatMap(td => {
//     for (x <- td.words; y <- td.hashtags; z <- List(td.polarity)) yield (x,y,z)
//   })
//   
//   val zipped = fm.zipWithUniqueId
//   (zipped.groupBy(r => r._1._2)
//         .map(r => (r._1, r._2.count(x => true)))
//         .sortBy(r => r._2, ascending=false)
//         .collect)
//
// }
