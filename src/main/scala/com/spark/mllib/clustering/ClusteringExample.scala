package com.spark.mllib.clustering

import org.apache.spark.mllib.clustering.{ KMeans, KMeansModel }
import org.apache.spark.mllib.linalg.Vectors
import com.spark.driver.SparkDriver

object ClusteringExample {
  def main(args: Array[String]) {

    val sparkSession = SparkDriver.getSparkSession("Clustering example")
    val data = sparkSession.sparkContext.textFile("src/main/resources/mllib/kmeans_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

  }
}