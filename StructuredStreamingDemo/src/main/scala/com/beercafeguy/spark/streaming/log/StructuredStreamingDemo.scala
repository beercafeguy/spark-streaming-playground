package com.beercafeguy.spark.streaming.log

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.regex.Pattern

import Utilities._


object StructuredStreamingDemo {

  case class LogEntry(ip:String,client:String,dateTime:String,request:String,status:String,bytes:String,referer:String,agent:String)

    val logPattern=apacheLogPattern()
    val datePattern=Pattern.compile("\\[(.*?) .+]")

    def parseDateField(field:String):Option[String]={
      val dateMatcher=datePattern.matcher(field)
      if(dateMatcher.find()){
        val dateString=dateMatcher.group(1)
        val dateFormat=new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss",Locale.ENGLISH)
        val date=dateFormat.parse(dateString)
        val timeStamp=new Timestamp(date.getTime)
        Option(timeStamp.toString)
      }else{
        None
      }
    }

}
