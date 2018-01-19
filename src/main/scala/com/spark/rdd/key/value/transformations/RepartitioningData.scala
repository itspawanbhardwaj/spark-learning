package com.spark.rdd.key.value.transformations

import com.spark.driver.SparkDriver

object RepartitioningData {
  def main(args: Array[String]) {

    val sparkSession = SparkDriver.getSparkSession("");
    sparkSession.range(1L, 100000L).repartition(4).write.partitionBy("");

    import org.apache.spark.sql.functions._
    import sparkSession.implicits._
    val rdd = sparkSession.sparkContext.parallelize(List(("user1", 1, 1.0), ("user1", 2, 2.0), ("user2", 1, 3.0), ("user2", 2, 4.0), ("user3", 1, 5.0), ("user3", 2, 6.0))) //.toDF("StudenID", "Right", "Wrong")

    val rdd1 = rdd.map(value => (value._1, value._2)).groupByKey.flatMap(value => List(value._1 , value._2))

    rdd1.collect.foreach(println)
  }
}