package com.beercageguy.spark

import com.beercageguy.spark.commons.BeerCafeSparkStreams

/**
 * Hello world!
 *
 */
object SimpleApp{
  def main(args: Array[String]): Unit = {
    //validate parquet op
    val spark=BeerCafeSparkStreams.streamingSession
    spark.read.parquet("output/*.parquet")
      .show()
  }
}
