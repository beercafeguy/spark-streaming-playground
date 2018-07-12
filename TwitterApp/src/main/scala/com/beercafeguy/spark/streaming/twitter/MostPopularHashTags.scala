package com.beercafeguy.spark.streaming.twitter

import java.util.concurrent.atomic.AtomicLong

import com.beercafeguy.spark.streaming.twitter.TwitterSetupUtils.{setupLogging, setupTwitter}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils

object MostPopularHashTags {

  def main(args: Array[String]): Unit = {
    setupTwitter()
    val ssc=new StreamingContext("local[*]","MostPopularHashTag",Seconds(1))

    setupLogging()
    val tweets=TwitterUtils.createStream(ssc,None)
    val statuses=tweets.map(status => status.getText)
    val tweetWords=statuses.flatMap(tweet => tweet.split(" "))
    val hashTags=tweetWords.filter(word => word.startsWith("#"))
    val hashTagMap=hashTags.map(h => (h,1))
    val hashTagCounts=hashTagMap.reduceByKeyAndWindow((x,y) => x+y,(x,y) => x-y,Seconds(120),Seconds(10))
    val sortedResults=hashTagCounts.transform(rdd => rdd.sortBy(_._2,false))
    sortedResults.print

    ssc.checkpoint("checkpoint_dir/")
    ssc.start()
    ssc.awaitTermination()
  }
}
