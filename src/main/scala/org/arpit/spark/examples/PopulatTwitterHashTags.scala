package org.arpit.spark.examples

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.twitter.TwitterUtils

object PopulatTwitterHashTags {

  def main(args: Array[String]): Unit = {
    setupTwitter()
    val ssc = new StreamingContext("local[*]", "PopulatTwitterHashTags", Seconds(1))
    setupLogging()
    
    val tweets = TwitterUtils.createStream(ssc, None)
    val sortedResults = tweets.map(status => status.getText())
                                 .flatMap(tweetText => tweetText.split(" "))
                                 .filter(word => word.startsWith("#"))
                                 .map(hashtag => (hashtag, 1))
                                 .reduceByKeyAndWindow((x, y) => x + y, (x, y) => x - y, Seconds(300), Seconds(5))
                                 .transform(rdd => rdd.sortBy(x => x._2, false))
    sortedResults.print
    startSparkStreaming(ssc)
  }
  
  
  def startSparkStreaming(ssc : StreamingContext) = {
    ssc.checkpoint("/home/arathore/checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }

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