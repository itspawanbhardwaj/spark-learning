package com.rdd.api.example

import org.apache.spark.SparkContext
/*
Listing Variants

def groupBy[K: ClassTag](f: T => K): RDD[(K, Iterable[T])]
def groupBy[K: ClassTag](f: T => K, numPartitions: Int): RDD[(K, Iterable[T])]
def groupBy[K: ClassTag](f: T => K, p: Partitioner): RDD[(K, Iterable[T])]
*/
object groupBy {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "groupBy")
    val a = sc.parallelize(1 to 9, 3)
    a.groupBy(x => { if (x % 2 == 0) "even" else "odd" }).collect.foreach(x => println("groups are : " + x))
    a.groupBy(x => x % 2).collect.foreach(x => println("groups are : " + x))
  }
}