package com.spark.mllib.variables

import org.apache.spark.mllib.util.MLUtils
import com.spark.driver.SparkDriver
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.GradientBoostedTrees

object GBTClassificationExample {
  def main(args: Array[String]) {
    val sparkSession = SparkDriver.getSparkSession("Principal Component Example")
    val data = MLUtils.loadLibSVMFile(sparkSession.sparkContext, "src/main/resources/mllib/sample_libsvm_data.txt");

    val splits = data.randomSplit(Array(.6, .4), seed = 11L)

    val (training, test) = (splits(0).cache(), splits(1))

    // Train a gradient boosted trees model
    // the default params for classification use logLoss by default

    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.numIterations = 3
    boostingStrategy.treeStrategy.numClasses = 2
    boostingStrategy.treeStrategy.maxDepth = 5

    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

    val model = GradientBoostedTrees.train(training, boostingStrategy)

    // evaluate model on test insrances and compute test error

    val labelAndPreds = test.map(point => {
      val predictions = model.predict(point.features)
      (point.label, predictions)

    })

    val testErr = labelAndPreds.filter(r => r._1 != r._2).count() / test.count()

    println("Test Error = " + testErr)
    println("GBT model = " + model.toDebugString)

  }
}