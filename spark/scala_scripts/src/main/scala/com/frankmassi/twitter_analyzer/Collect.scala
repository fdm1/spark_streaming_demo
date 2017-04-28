package com.frankmassi.twitter_analyzer

import com.google.gson.Gson
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status

/**
 * Collect at least the specified number of tweets into json text files.
 */

// val pipe_separator = "TWEETSPLITTER"

object rddFunctions {
    
  def getRawTweetText(status: twitter4j.Status): String = {
    if (status.isRetweet) { status.getRetweetedStatus().getText() }
    else { status.getText() }
  }

  def getCleanedStatusText(text: String): String = {
    val cleaner_regex = "(^rt |(^| )@\\S*|http\\S*|[^a-z0-9 #])".r
    text + "TWEETSPLITTER" + cleaner_regex.replaceAllIn(text.toLowerCase, "").trim
  }

}


object Collect {
  private var numTweetsToCollect = 10000L
  private var numTweetsCollected = 0L
  private var gson = new Gson()
  private var intervalSecs = 5
  private var filter_words = Array(sys.env("tweet_topics"))

  case class TweetData(raw_text: String,
                       filtered_words: Array[String],
                       hashtags: Array[String],
                       polarity: Float)


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
                        .transform(rdd => rdd.pipe("python ./pipe_scripts/wordlist_sentiment.py " + "TWEETSPLITTER"))
                        .map(piped_json => gson.fromJson(piped_json, classOf[TweetData]))
                        //
                        // .flatMap(tweetText => tweetText.split(" ").toSet)
                        // .map(word => word.trim)
                        // .filter(word => word.length > 1)
    
    tweetWords.window(Seconds(300), Seconds(5)).print
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
