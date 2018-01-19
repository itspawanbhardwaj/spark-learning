package com.spark.mllib.variables

import org.apache.spark.mllib.util.MLUtils
import com.spark.driver.SparkDriver
import org.apache.spark.mllib.tree.RandomForest

object RandomForestRegressionExample {
  def main(args: Array[String]) {
    val sparkSession = SparkDriver.getSparkSession("Principal Component Example")
    val data = MLUtils.loadLibSVMFile(sparkSession.sparkContext, "src/main/resources/mllib/sample_libsvm_data.txt");

    val splits = data.randomSplit(Array(.6, .4), seed = 11L)

    val (training, test) = (splits(0).cache(), splits(1))

    val numclasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 3
    val featureSubsetStrategy = "auto"
    val impurity = "variance"
    val maxDepth = 4
    val maxBins = 32
    val model = RandomForest.trainRegressor(training, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    val labelsAndPredictions = test.map(point => {
      val predictions = model.predict(point.features)
      (point.label, predictions)
    })

    val testMSE = labelsAndPredictions.map {
      case (v, p) => Math.pow((v - p), 2)

    }.mean()

    println("MSE : " + testMSE)
  }
}