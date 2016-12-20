package com.big.data.university.lab2

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.spark.driver.SparkDriver

object BDULab2 {

  def main(args: Array[String]) {
    val sparkSession = SparkDriver.getSparkSession("")
    val input1 = sparkSession.sparkContext.textFile("src/main/resources/bdu/trips/*");
    val header1 = input1.first

    //header1.foreach(println)

    val trips = input1.filter(_ != header1).map(_.split(","))

    val input2 = sparkSession.sparkContext.textFile("src/main/resources/bdu/stations/*")
    val header2 = input2.first

    val stations = input2.filter(_ != header2).map(_.split(",")).keyBy(_(0).toInt)
      .partitionBy(new HashPartitioner(trips.partitions.size))
    //stations.collect.foreach(println)

    val startTrips = stations.join(trips.keyBy(_(4).toInt))

    println(startTrips.toDebugString)

    val endTrips = stations.join(trips.keyBy(_(7).toInt))

    println(endTrips.toDebugString);

    println(startTrips.count)
    println(endTrips.count)
  }
}