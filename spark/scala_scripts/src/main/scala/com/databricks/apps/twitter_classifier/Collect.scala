// https://databricks.gitbooks.io/databricks-spark-reference-applications/content/twitter_classifier/collect.html
package com.databricks.apps.twitter_classifier

import java.io.File

import com.google.gson.Gson
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Collect at least the specified number of tweets into json text files.
 */
object Collect {
  private var numTweetsToCollect = 1000L
  private var numTweetsCollected = 0L
  private var partNum = 1
  private var gson = new Gson()
  private var intervalSecs = 10
  private var partitionsEachInterval = 1

  def main(args: Array[String]) {
    // Process program arguments and set properties
    // if (args.length < 3) {
    //   System.err.println("Usage: " + this.getClass.getSimpleName +
    //     "<outputDirectory> <numTweetsToCollect> <intervalInSeconds> <partitionsEachInterval>")
    //   System.exit(1)
    // }
    // val Array(outputDirectory, Utils.IntParam(numTweetsToCollect),  Utils.IntParam(intervalSecs), Utils.IntParam(partitionsEachInterval)) =
    //   Utils.parseCommandLineWithTwitterCredentials(args)
    // val outputDir = new File(outputDirectory.toString)
    // if (outputDir.exists()) {
    //   System.err.println("ERROR - %s already exists: delete or specify another directory".format(
    //     outputDirectory))
    //   System.exit(1)
    // }
    // outputDir.mkdirs()

    println("Initializing Streaming Spark Context...")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(intervalSecs))

    val tweetStream = TwitterUtils.createStream(ssc, Utils.getAuth).map(gson.toJson(_))

    tweetStream.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        val outputRDD = rdd.repartition(partitionsEachInterval)
        // outputRDD.saveAsTextFile(outputDirectory + "/tweets_" + time.milliseconds.toString)
        outputRDD.take(10).foreach(println)
        println(count)
        numTweetsCollected += count
        if (numTweetsCollected > numTweetsToCollect) {
          System.exit(0)
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
