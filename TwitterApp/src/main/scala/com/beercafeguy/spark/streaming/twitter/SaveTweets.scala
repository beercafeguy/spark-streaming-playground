package com.beercafeguy.spark.streaming.twitter

import TwitterSetupUtils._
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SaveTweets {

  def main(args: Array[String]): Unit = {
    setupTwitter()
    val ssc=new StreamingContext("local[*]","SaveTweets",Seconds(2))

    setupLogging()

    val tweets=TwitterUtils.createStream(ssc,None)
    val statuses=tweets.map(status => status.getText)

    //save every partition of every RDD of every stream in new files
    //statuses.saveAsTextFiles("Tweets","txt")

    var totalTweets:Long=0
    statuses.foreachRDD((rdd,time) =>{
      if(rdd.count >0){
        val repartitionedRDD=rdd.repartition(1).cache()
        repartitionedRDD.saveAsTextFile("output/Tweets_"+time.milliseconds.toString)
        totalTweets+=repartitionedRDD.count
        println("Tweets count:"+totalTweets)
        if(totalTweets>1000){
          System.exit(0)
        }
      }
    })

    ssc.checkpoint("checkpoint_dir/")
    ssc.start()
    ssc.awaitTermination()
  }
}
