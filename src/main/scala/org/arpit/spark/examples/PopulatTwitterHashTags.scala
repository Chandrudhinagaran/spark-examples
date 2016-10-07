package org.arpit.spark.examples

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.twitter.TwitterUtils

object PopulatTwitterHashTags {

  def main(args: Array[String]): Unit = {
    Utility.setupTwitter()
    val ssc = new StreamingContext("local[*]", "PopulatTwitterHashTags", Seconds(1))
    Utility.setupLogging()
    
    val tweets = TwitterUtils.createStream(ssc, None)
    val sortedResults = tweets.map(status => status.getText())
                                 .flatMap(tweetText => tweetText.split(" "))
                                 .filter(word => word.startsWith("#"))
                                 .map(hashtag => (hashtag, 1))
                                 .reduceByKeyAndWindow((x : Int, y:Int) => x + y, Seconds(300), Seconds(5))
                                 .transform(rdd => rdd.sortBy(x => x._2, false))
    sortedResults.print
    startSparkStreaming(ssc)
  }
  
  def startSparkStreaming(ssc : StreamingContext) = {
    ssc.checkpoint("/home/arathore/checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}