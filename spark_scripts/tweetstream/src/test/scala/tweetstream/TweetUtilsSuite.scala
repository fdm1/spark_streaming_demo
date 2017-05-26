package tweetstream

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.scalatest.mock.MockitoSugar

import org.mockito.Mockito._
import twitter4j.Status

@RunWith(classOf[JUnitRunner])
class TweetUtilsSuite extends FunSuite with BeforeAndAfterAll {
	def initializeTweetUtilsSuite(): Boolean =
    try {
      TweetUtils
      true
    } catch {
      case ex: Throwable =>
        println(ex.getMessage)
        ex.printStackTrace()
        false
    }

  override def afterAll(): Unit = {
    assert(initializeTweetUtilsSuite())
    // import TweetStream._
    // sc.stop()
  }
	
	val FAKE_TEXT = "I'm a fake tweet"

	def getMockTweet(isRetweet: Boolean): Status = {
		val status = mock(classOf[Status])
		when(status.getText()).thenReturn(FAKE_TEXT);
		status
	}

	test("'getRawTweetText' extracts a tweet's text") {
    assert(TweetUtils.getRawTweetText(getMockTweet(false)) == FAKE_TEXT)
  }



}
