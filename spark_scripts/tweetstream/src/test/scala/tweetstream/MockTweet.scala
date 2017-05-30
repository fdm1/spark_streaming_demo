package tweetstream

import org.mockito.Mockito.{mock, when}
import org.scalatest.mock.MockitoSugar
import twitter4j.Status


object MockTweet {

  val FAKE_STATUS_TEXT = "I'm a fake tweet"

  def getMockTweet(status_text: String = FAKE_STATUS_TEXT, isRetweet: Boolean = false): Status = {
    val status = mock(classOf[Status])
    when(status.getText()).thenReturn(status_text);
    status
  }
}
