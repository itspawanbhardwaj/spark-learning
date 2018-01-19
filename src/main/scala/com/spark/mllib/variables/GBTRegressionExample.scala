package com.spark.mllib.variables

import org.apache.spark.mllib.util.MLUtils
import com.spark.driver.SparkDriver
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.GradientBoostedTrees

object GBTRegressionExample {
  def main(args: Array[String]) {
    val sparkSession = SparkDriver.getSparkSession("Principal Component Example")
    val data = MLUtils.loadLibSVMFile(sparkSession.sparkContext, "src/main/resources/mllib/sample_libsvm_data.txt");

    val splits = data.randomSplit(Array(.6, .4), seed = 11L)

    val (training, test) = (splits(0).cache(), splits(1))

    // Train a gradient boosted trees model
    // the default params for classification use logLoss by default

    val boostingStrategy = BoostingStrategy.defaultParams("Regression")

    boostingStrategy.numIterations = 3
    boostingStrategy.treeStrategy.maxDepth = 5
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

    val model = GradientBoostedTrees.train(training, boostingStrategy)

    val labelAndPreds = test.map(point => {

      val predictions = model.predict(point.features)
      (point.label, predictions)

    })

    val testMSE = labelAndPreds.map {
      case (v, p) => Math.pow((v - p), 2)

    }.mean()

    println("mse : " + testMSE)
    println("model : " + model.toDebugString)

  }
}