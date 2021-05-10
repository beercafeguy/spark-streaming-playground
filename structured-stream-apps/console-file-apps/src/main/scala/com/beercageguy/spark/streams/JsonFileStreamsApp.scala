package com.beercageguy.spark.streams

import com.beercageguy.spark.commons.BeerCafeSparkStreams
import BeerCafeSparkStreams.streamingSession.implicits._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object JsonFileStreamsApp {
  val jsonFileDir="data/json/"
  val opDir="output/"
  val checkPntDir="checkpoints/"

  def main(args: Array[String]): Unit = {

    val spark = BeerCafeSparkStreams.streamingSession
    val inputStream=spark.readStream
      .format("json")
      .schema(getSchema())
      .load(jsonFileDir)

    val resultDF=inputStream.select("value").withColumn("square",$"value"*$"value")

    val streamingQuery=resultDF.writeStream
      .format("parquet")
      .option("path",opDir)
      .option("checkpointLocation",checkPntDir)
      .start()

    streamingQuery.awaitTermination()
  }

  def getSchema():StructType={
    new StructType()
      .add(StructField("key",IntegerType))
      .add(StructField("value",IntegerType))
  }
}
