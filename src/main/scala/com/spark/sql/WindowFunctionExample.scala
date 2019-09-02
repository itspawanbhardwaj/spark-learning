package com.spark.sql

import com.spark.driver.SparkDriver
import org.apache.spark.sql.expressions.Window

object WindowFunctionExample {
  def main(args: Array[String]) {
    val spark = SparkDriver.getSparkSession("window functions")
    val array = Array(
      ("Thin", "Cell phone", 6000),
      ("Normal", "tablet", 1500),
      ("mini", "Cell phone", 5500),
      ("ultra thin", "Cell phone", 5000),
      ("very thin", "tablet", 6000),
      ("big", "Cell phone", 2500),
      ("bendable", "Cell phone", 3000),
      ("foldable", "Cell phone", 3000),
      ("Pro", "tablet", 4500),
      ("pro2", "tablet", 6500))

    import spark.implicits._

    val df = spark.sparkContext.parallelize(array).toDF("product", "category", "revenue")

    df.show

    df.createOrReplaceTempView("productrevenue")

    spark.sql("select count(*) from productrevenue").show
    spark.sql("""SELECT
  product,
  category,
  revenue
FROM (
  SELECT
    product,
    category,
    revenue,
    dense_rank() OVER (PARTITION BY category ORDER BY revenue DESC) as rank
  FROM productrevenue) tmp
WHERE
  rank <= 2""").show

    val window = Window.partitionBy($"category").orderBy($"revenue".desc)
    import org.apache.spark.sql.functions._

    val rankCol = rank().over(window)
    df.select(
      rankCol,
      $"product",
      $"category", $"revenue").withColumn("avg_value", avg($"revenue").over(window))
      .withColumn("avg_value diff", avg($"revenue").over(window) - $"revenue").show
  }
}