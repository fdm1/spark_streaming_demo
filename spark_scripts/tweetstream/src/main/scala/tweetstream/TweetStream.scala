package tweetstream

import com.google.gson.Gson
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils


object TweetStream {
  private var numTweetsToCollect = 60000L
  private var numTweetsCollected = 0L
  private var gson = new Gson()
  private var intervalSecs = 5
  private var filter_words = Array(sys.env("tweet_topics"))

  val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  val ssc = new StreamingContext(sc, Seconds(intervalSecs))

  def main(args: Array[String]) {
    println("Initializing Streaming Spark Context...")
    val tweetStream = TwitterUtils.createStream(ssc, TwitterAuthUtils.getAuth, filter_words)
    val rawStatusText = tweetStream.map(TweetUtils.getRawTweetText)
    
    rawStatusText.window(Seconds(10)).print

    ssc.checkpoint("/tmp/checkpoint")
    ssc.start()
    ssc.awaitTermination()

  }
}

// vim: set ts=4 sw=4 et:
