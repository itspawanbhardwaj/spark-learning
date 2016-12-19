package com.big.data.university.lab4

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
/*This lab will cover caching ans memory tuning . 
 * Start by caching the trips ans stations RDDs we used previously .
 *  Compare thr different storage levels including 
 *  serialized vs not and replication .
 *  Review the slides or documentstion for the storage levels*/
object caching {
  def main(args: Array[String]) = {
    val sc = new SparkContext("local", "DropTable")
    val sqlContext = new SQLContext(sc)
    val input1 = sc.textFile("src/main/resources/bdu/trips/*");
    val header1 = input1.first

    val trips = input1.
      filter(_ != header1).
      map(_.split(",")).
      map(com.big.data.university.lab3.utils.Trip.parse(_))

    //caching
    trips.cache
    val input2 = sc.textFile("src/main/resources/bdu/stations/*")
    val header2 = input2.first

    val stations = input2.
      filter(_ != header2).
      map(_.split(",")).
      map(com.big.data.university.lab3.utils.Station.parse(_))

    /*
Lets start by calculating the average duration of trips
 both by start AND end terminal in the trips RDD.
* */

    val durationByStart = trips.keyBy(_.startTerminal).mapValues(_.duration)
    val durationByEnd = trips.keyBy(_.endTerminal).mapValues(_.duration)

    val resultsStart = durationByStart.aggregateByKey((0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val avgStart = resultsStart.mapValues(value => value._1 / value._2)

    val resultsEnd = durationByEnd.aggregateByKey((0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val avgEnd = resultsEnd.mapValues(value => value._1 / value._2)

    avgStart.collect
    avgEnd.collect
    println("completed")
    /*without caching : 20sec*/
    /*with caching : 17sec*/
  }

}