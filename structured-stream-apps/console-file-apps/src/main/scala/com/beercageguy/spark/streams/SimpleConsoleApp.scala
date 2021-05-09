package com.beercageguy.spark.streams

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, split}

/**
 * nc -lk 9999 is used for starting netcat server
 */
object SimpleConsoleApp {
  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
      .appName("Simple Console App")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions",4)
      .getOrCreate()
    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "172.29.8.50")
      .option("port", 9999)
      .load()

    lines.printSchema()
    val countStream=lines.select(explode(split($"value"," ")).as("word"))
      .groupBy("word")
      .count

    val query = countStream.writeStream
      .outputMode("complete")
      //.outputMode("append") -> Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark
      .format("console")
      .start()

    query.awaitTermination()
  }
}
