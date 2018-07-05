package com.beercafeguy.spark.streaming.twitter

import java.util.regex.Pattern

import org.apache.log4j.{Level, Logger}

import scala.io.Source

object TwitterSetupUtils {

  //setting log level for the APP
  def setupLogging()={
    val rootLogger=Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
  }

  def setupTwitter()={
    for(line <-Source.fromFile("twitter.txt").getLines()){
      val fields=line.split(" ")
      if(fields.length==2){
        System.setProperty("twitter4j.oauth."+fields(0),fields(1))
      }
    }
  }

  /** Retrieves a regex Pattern for parsing Apache access logs. */
  def apacheLogPattern():Pattern = {
    val ddd = "\\d{1,3}"
    val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"
    val client = "(\\S+)"
    val user = "(\\S+)"
    val dateTime = "(\\[.+?\\])"
    val request = "\"(.*?)\""
    val status = "(\\d{3})"
    val bytes = "(\\S+)"
    val referer = "\"(.*?)\""
    val agent = "\"(.*?)\""
    val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
    Pattern.compile(regex)
  }
}
