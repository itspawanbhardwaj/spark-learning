package com.spark.rdd.key.value.transformations

import com.spark.driver.SparkDriver
import org.apache.spark.sql.functions._
object TaskOnOneExecutor {
  def main(args: Array[String]) {

    val sparkSession = SparkDriver.getSparkSession("Spark Example");

    val left = sparkSession.range(1L, 100000L).select(lit(1L), rand(1)).toDF("k", "v")
    left.show

    val right = sparkSession.range(1L, 100000L).select(
      (rand(3) * 1000).cast("bigint"), rand(1)).toDF("k", "v")
    right.show

    import sparkSession.implicits._
    type KeyType = Long
    val keys = left.select($"k").distinct.as[KeyType].collect

    val rightFiltered = broadcast(right.where($"k".isin(keys: _*)))
    left.join(broadcast(rightFiltered), Seq("k"))

    sparkSession.conf.set("spark.sql.crossJoin.enabled", true)

    left.as("left")
      .join(rightFiltered.as("right"))
      .where($"left.k" === $"right.k")
  }
}