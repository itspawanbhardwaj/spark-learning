package com.big.data.university.lab3

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext

object TestUtils {

  def main(args: Array[String]) {
    val sc = new SparkContext("local", "DropTable")
    val sqlContext = new SQLContext(sc)
    val input1 = sc.textFile("src/main/resources/bdu/trips/*");
    val header1 = input1.first

    val trips = input1.
      filter(_ != header1).
      map(_.split(",")).
      map(com.big.data.university.lab3.utils.Trip.parse(_))

    val input2 = sc.textFile("src/main/resources/bdu/stations/*")
    val header2 = input2.first

    val stations = input2.
      filter(_ != header2).
      map(_.split(",")).
      map(com.big.data.university.lab3.utils.Station.parse(_))

    val byStartTerminal = trips.keyBy(_.startStation)
    val durationByStart = byStartTerminal.mapValues(_.duration)
    /* val grouped = durationByStart.groupByKey().mapValues(list => list.sum / list.size)
    grouped.take(10).foreach(println)*/

    val results = durationByStart.
      aggregateByKey((0, 0))(
        (acc, value) => //acc = (0,0) and value = duration
          (acc._1 + value, acc._2 + 1),
        (acc1, acc2) => 
          (acc1._1 + acc2._1, acc1._2 + acc2._2))

    results.mapValues(values => values._1 / values._2).take(10).foreach(println)
  }
}