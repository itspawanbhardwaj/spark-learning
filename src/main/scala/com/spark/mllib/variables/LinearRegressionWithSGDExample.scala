package com.spark.mllib.variables

import org.apache.spark.mllib.util.MLUtils
import com.spark.driver.SparkDriver
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

object LinearRegressionWithSGDExample {
  def main(args: Array[String]) {
    val sparkSession = SparkDriver.getSparkSession("Principal Component Example")

    // Load and parse the data
    val data = sparkSession.sparkContext.textFile("src/main/resources/mllib/lpsa.data")

    val parsedData = data.map(line => {
      val parts = line.split(',')

      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble)))
    }).cache()

    // building the model

    val numIterations = 100

    val model = LinearRegressionWithSGD.train(parsedData, numIterations)

    val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val MSE = valuesAndPreds.map { case (v, p) => Math.pow((v - p), 2) }

    println("training Mean Squared Error = " + MSE.mean())

  }
}