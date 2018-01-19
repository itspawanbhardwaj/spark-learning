package com.rdd.api.example

import org.apache.spark.SparkContext

object getStorageLevel {
  def main(args: Array[String]) {
    val sc = new SparkContext("local", "getStorageLevel")
    val a = sc.parallelize(1 to 100000, 2)
    a.persist(org.apache.spark.storage.StorageLevel.DISK_ONLY)

    println(a.getStorageLevel.description);
  }
}