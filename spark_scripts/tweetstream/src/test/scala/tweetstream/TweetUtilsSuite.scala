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
	
	val FAKE_STATUS_TEXT = "I'm a fake tweet"

	def getMockTweet(status_text: String = FAKE_STATUS_TEXT, isRetweet: Boolean = false): Status = {
		val status = mock(classOf[Status])
		when(status.getText()).thenReturn(status_text);
		status
	}

	def foobar(s: String = ""): String = { "foo" }

	test("'getRawTweetText' extracts a tweet's text") {
    assert(TweetUtils.getRawTweetText(getMockTweet()) == FAKE_STATUS_TEXT)
  }
}
