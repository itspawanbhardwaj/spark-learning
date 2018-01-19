package com.spark.mllib.variables

import org.apache.spark.mllib.util.MLUtils
import com.spark.driver.SparkDriver
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.evaluation.MulticlassMetrics

object LogisticRegressionExample {
  def main(args: Array[String]) {

    val sparkSession = SparkDriver.getSparkSession("Principal Component Example")

    val data = MLUtils.loadLibSVMFile(sparkSession.sparkContext, "src/main/resources/mllib/sample_libsvm_data.txt");

    val splits = data.randomSplit(Array(.6, .4), seed = 11L)

    val (training, test) = (splits(0).cache(), splits(1))

    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(10)
      .run(training)

    //compute raw scores on the test set.

    val predictionAndLabels = test.map {
      case LabeledPoint(label, feature) =>
        val prediction = model.predict(feature)
        (prediction, label)
    }

    // Get evaluation matrices
    val metrices = new MulticlassMetrics(predictionAndLabels)

    val precision = metrices.precision

    println("Precision: " + precision)

  }
}