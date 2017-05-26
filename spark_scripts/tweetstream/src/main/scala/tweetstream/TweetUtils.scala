package tweetstream

import twitter4j.Status

object TweetUtils {
  // private var tweetSeparatorString = "TWEETSPLITTER"

  def getRawTweetText(status: twitter4j.Status): String = {
    // if (status.isRetweet) { status.getRetweetedStatus().getText() + tweetSeparatorString + "RETWEET" }
    if (status.isRetweet) { status.getRetweetedStatus().getText() }
    else { status.getText() }
  }
}

// vim: set ts=4 sw=4 et:
