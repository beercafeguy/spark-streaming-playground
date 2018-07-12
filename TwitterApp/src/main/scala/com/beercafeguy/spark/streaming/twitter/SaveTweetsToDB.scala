package com.beercafeguy.spark.streaming.twitter

import java.sql.{Connection, DriverManager}

import com.beercafeguy.spark.streaming.twitter.TwitterSetupUtils.{setupLogging, setupTwitter}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils

object SaveTweetsToDB {

  def main(args: Array[String]): Unit = {
    setupTwitter()
    val ssc=new StreamingContext("local[*]","SaveTweetsToDB",Seconds(2))

    setupLogging()

    val tweets=TwitterUtils.createStream(ssc,None)
    val statuses=tweets.map(status => (status.getId,status.getText))


    var totalTweets:Long=0
    statuses.foreachRDD((rdd,time) =>{
      if(rdd.count >0){
        val repartitionedRDD=rdd.repartition(1).cache()
        repartitionedRDD.foreachPartition(partitionOfRecords =>{
          Class.forName("com.mysql.jdbc.Driver")
          val connection=DriverManager.getConnection("jdbc:mysql://ms.beercafeguy.com:3306/retail_db","sqoopuser","sqoop")
          val del = connection.prepareStatement ("INSERT INTO tweets (text,createdon) VALUES (?,?)")
          partitionOfRecords.foreach(record => {
            del.setString(1,record._2)
            del.setString(2,time.milliseconds.toString)
            del.executeUpdate()
          })
        })

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
