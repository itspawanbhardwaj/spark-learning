package com.spark.mllib.variables

import com.spark.driver.SparkDriver

object LinearSupportVectorMachinesExample {
  def main(args: Array[String]) {
    println("lets rock !!");
    import org.apache.spark.mllib.classification.{ SVMModel, SVMWithSGD }
    import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
    import org.apache.spark.mllib.util.MLUtils
    val sparkSession = SparkDriver.getSparkSession("Principal Component Example")

    val data = MLUtils.loadLibSVMFile(sparkSession.sparkContext, "src/main/resources/mllib/sample_libsvm_data.txt");

    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)

    val training = splits(0).cache()

    val test = splits(1)

    val numIterations = 100

    val model = SVMWithSGD.train(training, numIterations)
    // clear default threshold
    model.clearThreshold();

    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics

    val metrics = new BinaryClassificationMetrics(scoreAndLabels)

    val auROC = metrics.areaUnderROC()

    println("area under ROC: " + auROC)

  }
}