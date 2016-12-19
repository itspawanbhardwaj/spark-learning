package com.spark.rdd.key.value.transformations

import com.spark.driver.SparkDriver

object MapPartitionExample {
  def main(args: Array[String]) {
    println("pawan bhardwaj");
    //mapPartitions() is called once for each Partition unlike map()
    val sparkContext = SparkDriver.getSparkSession("Flat map example").sparkContext;
    val fileRDD = sparkContext.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3);

    import scala.runtime.ScalaRunTime._
    fileRDD.mapPartitions(lines => List(lines.next).iterator, true).collect.foreach(println)

   
    fileRDD.mapPartitions(lines => lines, true).collect.foreach(println)

    val mapped = fileRDD.mapPartitionsWithIndex {
      // 'index' represents the Partition No
      // 'iterator' to iterate through all elements
      //  
      (index, iterator) =>
        {
          println("Called in Partition -> " + index)
          val myList = iterator.toList
          // In a normal user case, we will do the
          // the initialization(ex : initializing database)
          // before iterating through each element
          myList.map(x => x + " -> " + index).iterator
        }
    }.collect.foreach(println)

  }
}