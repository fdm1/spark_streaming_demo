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
    windowedRDD.map(r => r.word).countByValue(1).print
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
// {"uuid":"049521f2-972f-43fb-b4f1-e34f2684c17d","raw_text":"https://t.co/iadEDv4miK","words":["againpresid","pdb","daili","bannon","strike"],"hashtags":[""],"is_retweet":"true"}
// {"uuid":"06179aef-7eb1-443d-b758-6d03b02f1b3a","raw_text":"Republican Sens. John McCain, Lindsey Graham ask Pres. Trump for evidence of wiretapping claims… https://t.co/kQauG5O6rK","words":["ask","evid","sen","republican","wiretap","mccain","claim","john","lindsey","graham","pre"],"hashtags":[""],"is_retweet":"true"}
// {"uuid":"d7d0a458-ae0d-4bbe-97cd-bd730d9477aa","raw_text":"The Trump White House: \"a well-oiled machine.\" Oiled apparently by Exxon-Mobil. https://t.co/DbBBW2zh6H","words":["appar","white","oil","hou","machin","welloil","exxonmobil"],"hashtags":[""],"is_retweet":"true"}
// {"uuid":"9c38e7a3-e9e6-4fc0-afaf-d8a390c3f58e","raw_text":"FBI Director James Comey was \"incredulous\" over President Trump\u0027s tweets, a source says https://t.co/Aop0fNfxuH https://t.co/5FJEGS30mo","words":["sourc","director","fbi","tweet","jame","comey","incredul"],"hashtags":[""],"is_retweet":"true"}
// {"uuid":"023cf49c-1f59-408b-bebf-699b34d0c429","raw_text":"@IvankaTrump  @realDonaldTrump CONGRATULATIONS on your nomination for Nobel Peace Prize Mr. President!… https://t.co/Sk82nia5RH","words":["nomin","nobel","congratul","presid","prize","peac"],"hashtags":[""],"is_retweet":"false"}
// {"uuid":"8b43eba5-2cd1-41c6-bf3f-5d1593f1126d","raw_text":"@SpeakerRyan Read this. This is the real danger Americans are facing today, the fear they rightly feel. How do we f… https://t.co/qkymiyI0yB","words":["face","danger","read","american","feel","right","fear","today","real"],"hashtags":[""],"is_retweet":"false"}
// {"uuid":"64fd7ef2-1bf3-420c-970e-59de1743f8a3","raw_text":"@MalcolmNance @morningmika @amjoyshow Her friendship and devotion to Trump. It\u0027s hard to see your friend deteriorate in front of ur👀","words":["deterior","friend","front","see","devot","friendship","hard"],"hashtags":[""],"is_retweet":"false"}
// {"uuid":"583012d2-20ca-483a-bf1f-4227d1209d25","raw_text":"https://t.co/kLWwtDxa6R","words":["protect","stop","know","claim","attack","truth","non"],"hashtags":[""],"is_retweet":"true"}
// {"uuid":"ca7d5d44-9019-49f3-afe0-0fe9b286c402","raw_text":"I am calling for a total and complete shutdown on Donald Trump being POTUS until we can figure out what the hell is going on","words":["complet","hell","potu","total","shutdown","figur","call"],"hashtags":[""],"is_retweet":"true"}
// {"uuid":"c6a10364-9d72-429b-9961-e33f821f1f47","raw_text":"DISGUSTING TRUMP \u0026 REPUBLICANS!!! Trump will kill the Great Lakes: Stephen Henderson https://t.co/gYsUQWWKTp via @USATODAY","words":["disgust","vium","republican","stephen","lake","kill","great","henderson"],"hashtags":[""],"is_retweet":"false"}


// val tweets = sc.parallelize(t)
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
