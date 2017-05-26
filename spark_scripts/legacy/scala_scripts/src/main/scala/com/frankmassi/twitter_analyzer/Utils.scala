package com.frankmassi.twitter_analyzer

import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object Utils {

  def setTwitterEnvProperties = {
    System.setProperty("twitter4j.oauth.consumerKey", sys.env("consumer_token"))
    System.setProperty("twitter4j.oauth.consumerSecret", sys.env("consumer_secret"))
    System.setProperty("twitter4j.oauth.accessToken", sys.env("access_token"))
    System.setProperty("twitter4j.oauth.accessTokenSecret", sys.env("access_token_secret"))
  }

  def getAuth = {
    setTwitterEnvProperties
    Some(new OAuthAuthorization(new ConfigurationBuilder().build()))
  }
}

