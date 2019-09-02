package com.pb.streaming.kafka

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object KafkaExample {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Spark Streaming").master("local[*]").getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    //Subscribe to multiple topics
    val multipleTopicdf = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "testTopic")
      .load()

    multipleTopicdf.show()
    multipleTopicdf.printSchema()
    val result1 = multipleTopicdf.selectExpr("CAST(value AS STRING)", "topic", "partition", "offset") //Same schema as what we get in readStream
      .as[(String, String, String, String)]
    result1.show()

    result1
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "sparktopic")
      .save()
  }

}