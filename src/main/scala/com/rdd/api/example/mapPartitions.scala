package com.rdd.api.example

import org.apache.spark.SparkContext

object mapPartitions {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "mapPartitions")
    //Example 1
    val a = sc.parallelize(1 to 9, 3)
    def myfunc[T](iter: Iterator[T]): Iterator[(T, T)] = {
      var res = List[(T, T)]()
      var pre = iter.next
      
      while (iter.hasNext) {
        val cur = iter.next;
        res.::=(pre, cur)
        pre = cur;
      }
      res.iterator
    }
    //a.mapPartitions(myfunc).collect.foreach(println)

    //Example 2
    val x = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3)
    def myfunc2(iter: Iterator[Int]): Iterator[Int] = {
      var res = List[Int]()
      while (iter.hasNext) {
        val cur = iter.next;
        res = res ::: List.fill(scala.util.Random.nextInt(10))(cur)
      }
      res.iterator
    }
    x.mapPartitions(myfunc2).collect.foreach(println)

    //Example 2 using flatmap

    val y = sc.parallelize(1 to 10, 3)
    y.flatMap(List.fill(scala.util.Random.nextInt(10))(_)).collect
  }
}