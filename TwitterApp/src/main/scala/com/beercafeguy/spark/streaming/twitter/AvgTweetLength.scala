package com.beercafeguy.spark.streaming.twitter

import java.util.concurrent.atomic.AtomicLong

import TwitterSetupUtils._
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
object AvgTweetLength {

  def main(args: Array[String]): Unit = {
    setupTwitter()
    val ssc=new StreamingContext("local[*]","AvgTweetLength",Seconds(1))

    setupLogging()
    val tweets=TwitterUtils.createStream(ssc,None)
    val statuses=tweets.map(status => status.getText)
    val lengths=statuses.map(status => status.length)

    val totalTweets:AtomicLong=new AtomicLong(0)
    val totalChars:AtomicLong=new AtomicLong(0)

    lengths.foreachRDD((rdd,time) => {
      val count=rdd.count()
      if(count>0){
        totalTweets.getAndAdd(count)
        totalChars.getAndAdd(rdd.reduce((x,y) => x+y))
        println("Total Tweets: "+totalTweets.get()
          +" Total chars: "+totalChars.get()
          + "Average: "+totalChars.get()/totalTweets.get())
      }
    })

    ssc.checkpoint("checkpoint_dir/")
    ssc.start()
    ssc.awaitTermination()
  }
}
