package org.arpit.spark.examples

object Utility {
  def setupLogging() = {
    import org.apache.log4j.{ Level, Logger }
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

  def setupTwitter() = {
    System.setProperty("twitter4j.oauth.consumerKey", "WFGoXZ8YVVZy1owb638xqwgds")
    System.setProperty("twitter4j.oauth.consumerSecret", "nbxIHKBEXeJGlyHVAk504IqU8BpCiowTyu8qaw3nHG5BFbITof")
    System.setProperty("twitter4j.oauth.accessToken", "756431275720863745-kZcz6W4bTL3Vz97fZWHHK9jCFUOH45w")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "u1sAnf6XYSljgjbK2nHmB6uml5B8xJec2xcwgrb32L8xY")
  }
}