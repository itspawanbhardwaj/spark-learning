package com.pb.streaming.kafka

import org.apache.spark.sql.SparkSession

object KafkaStreaming {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Spark Streaming1").master("local[*]").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    val multipleTopicdf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "testTopic")
      .load()

    val result1 = multipleTopicdf.selectExpr("CAST(value AS STRING)", "topic", "partition", "offset") //Same schema as what we get in readStream
    // .as[(String, String, String, String)]

    val query = result1.writeStream.format("console").start()
    query.awaitTermination()
  }
}