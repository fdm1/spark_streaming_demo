package tweetstream

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

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

	test("'getRawTweetText' extracts a tweet's text") {
    assert(TweetUtils.getRawTweetText(MockTweet.getMockTweet()) == MockTweet.FAKE_STATUS_TEXT)
  }
}
