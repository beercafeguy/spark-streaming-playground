package com.beercafeguy.spark.streaming.twitter
import java.util.concurrent.atomic.AtomicLong

import TwitterSetupUtils._
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object LongestTweetInBatch {

  def main(args: Array[String]): Unit = {
    setupTwitter()
    val ssc=new StreamingContext("local[*]","LongestTweet",Seconds(1))

    setupLogging()
    val tweets=TwitterUtils.createStream(ssc,None)
    val statuses=tweets.map(status => status.getText)
    val lengths=statuses.map(status => (status,status.length))

    val maxLength:AtomicLong=new AtomicLong(0)
    val totalChars:AtomicLong=new AtomicLong(0)

    lengths.foreachRDD((rdd,time) => {
      val count=rdd.count()
      if(count>0){
        val longestTweet=rdd.reduce((x,y) => if(x._2>y._2) x else y)
        println("Longest Tweet Length: "+longestTweet._2+" Text: "+longestTweet._1)
      }
    })

    ssc.checkpoint("checkpoint_dir/")
    ssc.start()
    ssc.awaitTermination()
  }
}
