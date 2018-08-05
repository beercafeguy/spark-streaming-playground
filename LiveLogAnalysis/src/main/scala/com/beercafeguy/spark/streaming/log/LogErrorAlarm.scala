package com.beercafeguy.spark.streaming.log

import java.util.regex.Matcher

import com.beercafeguy.spark.streaming.log.Utilities.{apacheLogPattern, setupLogging}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object LogErrorAlarm {

  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext("local[*]", "LogErrorAlarm", Seconds(1))

    setupLogging

    val pattern=apacheLogPattern()

    val lines=ssc.socketTextStream("localhost",9999,StorageLevel.MEMORY_AND_DISK_SER)
    val statuses=lines.map(line => {
      val matcher:Matcher=pattern.matcher(line)
      if(matcher.matches())
        matcher.group(6)
      else
        "{error}"
    })

    val successFailure=statuses.map( status => {
      val statusCode=util.Try(status.toInt) getOrElse(0)
      if(statusCode >=200 && statusCode <300)
        "Success"
      else if (statusCode >=500 && statusCode <600)
        "Failure"
      else
        "Other"
    })

    val statusCodeCount=successFailure.countByValueAndWindow(Seconds(300),Seconds(1))
    statusCodeCount.foreachRDD((rdd,time) =>{
      var totalSuccess:Long=0
      var totalError:Long=0

      if(rdd.count()>0){
        val elements=rdd.collect()
        for(element <- elements){
          val result=element._1
          val count=element._2
          if(result =="Success")
            totalSuccess+=count
          if(result =="Failure")
            totalError+=count
        }
      }

      println(s"Total Success: ${totalSuccess} :: Total Error: ${totalError}")
      if(totalError+totalSuccess>100){
        val ratio:Double=util.Try(totalError.toDouble/totalSuccess.toDouble) getOrElse(1.0)
        if(ratio>0.5)
          println("Wake up Admin! Something is horribly wrong")
        else
          println("All good.")
      }
    })


    ssc.checkpoint("./cp/")
    ssc.start()
    ssc.awaitTermination()
  }
}
