package com.spark.mllib.variables

import com.spark.driver.SparkDriver
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.tree.DecisionTree

object DecisionTreeClassificationExample {
  def main(args: Array[String]) {
    println("lets rock!!")

    val sparkSession = SparkDriver.getSparkSession("Principal Component Example")
    val data = MLUtils.loadLibSVMFile(sparkSession.sparkContext, "src/main/resources/mllib/sample_libsvm_data.txt");

    val splits = data.randomSplit(Array(.6, .4), seed = 11L)

    val (training, test) = (splits(0).cache(), splits(1))

    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(training, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

    // evaluation model in test instances and compute test error

    val labelAndPreds = test.map(point => {
      val prediction = model.predict(point.features)
      (point.label, prediction)
    })

    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / test.count()

    println("testError = " + testErr)
    println(model.toDebugString)
  }
}