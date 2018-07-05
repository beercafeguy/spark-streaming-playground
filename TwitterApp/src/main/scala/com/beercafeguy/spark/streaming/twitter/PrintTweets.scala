package com.beercafeguy.spark.streaming.twitter

import TwitterSetupUtils._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.spark.streaming.twitter.TwitterUtils

object PrintTweets extends App{

  setupTwitter()

  val ssc=new StreamingContext("local[*]","PrintTweets",Seconds(2))

  setupLogging()

  val tweets=TwitterUtils.createStream(ssc,None)
  val statuses=tweets.map(status => status.getText)
  statuses.print()

  ssc.start()
  ssc.awaitTermination()
}
