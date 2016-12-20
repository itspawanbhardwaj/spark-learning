package com.edx.spark.transformations

import org.apache.spark.SparkContext

object EdxTextFile {

  def main(args: Array[String]) {

    val sc = new SparkContext("local", "wordCounts")
    val lines = sc.textFile("src/main/resources/employees.csv", 4)
    /*lins containing M*/
    lines.filter(text => text.contains("M")).collect.foreach(println)
    /*lins not containing M*/
    lines.filter(text => !text.contains("M")).collect.foreach(println)

    println("no of lines in the file : " + lines.count);
    println("first line in the file : " + lines.first);
    println("no of lines containing M (records of male ) : " + lines.filter(text => text.contains("M")).count);

    println("the line with the most words : " + lines.map(line => line.split(",").size).reduce((a, b) => if (a > b) a else b))
    println("One common data flow pattern is MapReduce, as popularized by Hadoop. Spark can implement MapReduce flows easily:")
    val wordCounts = lines.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    println(wordCounts)
  }
}
