package com.spark.mllib.variables

import com.spark.driver.SparkDriver
import org.apache.spark.mllib.regression.IsotonicRegression

object IsotonicRegressionExample {
  def main(args: Array[String]) {

    val sparkSession = SparkDriver.getSparkSession("Principal Component Example")

    val data = sparkSession.sparkContext.textFile("src/main/resources/mllib/sample_isotonic_regression_data.txt");

    val parsedData = data.map {
      line =>

        val parts = line.split(',').map(_.toDouble)
        (parts(0), parts(1), 1.0)
    }

    // Split data into training

    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)

    val model = new IsotonicRegression().setIsotonic(true).run(training)

    val predictionAndLabel = test.map {
      point =>
        val predictedLabel = model.predict(point._2)
        (predictedLabel, point._1)
    }

    val meanSquareError = predictionAndLabel.map {
      case (v, p) =>
        math.pow(p - v, 2)

    }.mean()

    println("Mean squared error: " + meanSquareError)

  }
}