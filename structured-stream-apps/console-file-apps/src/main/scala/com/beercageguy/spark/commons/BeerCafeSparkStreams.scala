package com.beercageguy.spark.commons

import org.apache.spark.sql.SparkSession

object BeerCafeSparkStreams {

  val streamingSession=SparkSession
    .builder()
    .config("spark.sql.shuffle.partitions",4)
    .appName("Simpe Json File Streamer")
    .master("local[*]")
    .getOrCreate()
}
