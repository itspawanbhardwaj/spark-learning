package com.spark.mllib.variables

import com.spark.driver.SparkDriver
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.NaiveBayes

object NaiveBayesExample {

  def main(args: Array[String]) {
    val sparkSession = SparkDriver.getSparkSession("Principal Component Example")

    val data = sparkSession.sparkContext.textFile("src/main/resources/mllib/sample_naive_bayes_data.txt");
    val parsedData = data.map(line => {
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    })

    val splits = (parsedData.randomSplit(Array(.5, .5), seed = 11L))
    val (training, test) = (splits(0), splits(1))

    val model = NaiveBayes.train(training, lambda = 1.0)

    val predictAndLabel = test.map(point => {
      (model.predict(point.features), point.label)
    })
    println(predictAndLabel.count())
    predictAndLabel.collect.foreach(println)
    val accuracy = 1.0 * predictAndLabel.filter(x => x._1 == x._2).count() / test.count

    println("accuracy : " + accuracy)
  }
}