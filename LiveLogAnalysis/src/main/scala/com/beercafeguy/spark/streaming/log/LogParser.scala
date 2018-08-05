package com.beercafeguy.spark.streaming.log

import java.util.regex.Matcher

import org.apache.spark.streaming.{Seconds, StreamingContext}
import Utilities._
import org.apache.spark.storage.StorageLevel
object LogParser {

  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext("local[*]", "LogParser", Seconds(1))

    setupLogging

    val pattern=apacheLogPattern()

    val lines=ssc.socketTextStream("localhost",9999,StorageLevel.MEMORY_AND_DISK_SER)
    val requests=lines.map(line => {
      val matcher:Matcher=pattern.matcher(line)
      if(matcher.matches())
        matcher.group(5)
    })

    val urls=requests.map( log => {
      val arr=log.toString.split(" ")
      if(arr.size == 3)
        arr(1)
      else "{error}"
    })

    val urlCounts=urls.map(url => (url,1)).reduceByKeyAndWindow(_+_,_-_,Seconds(300),Seconds(1))
    val sortedURLs=urlCounts.transform(rdd => rdd.sortBy( x => x._2,false))

    sortedURLs.print
    ssc.checkpoint("./cp/")
    ssc.start()
    ssc.awaitTermination()
  }
}
