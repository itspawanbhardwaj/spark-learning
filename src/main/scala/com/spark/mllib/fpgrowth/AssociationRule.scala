package com.spark.mllib.fpgrowth

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.mllib.fpm.AssociationRules
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.sql.SQLContext
object AssociationRule {

  def main(args: Array[String]) {
    val sc = new SparkContext("local[*]", "English Wikipedia pageviews by second")
    val sqlContext = new SQLContext(sc)

    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);

    val freqItemSets = sc.parallelize(Seq(
      new FreqItemset(Array("a"), 15L),
      new FreqItemset(Array("b"), 35L),
      new FreqItemset(Array("a", "b"), 35L),
      new FreqItemset(Array("a", "b"), 35L),
      new FreqItemset(Array("a", "b"), 35L),
      new FreqItemset(Array("a", "b"), 12L)))

    val ar = new AssociationRules().setMinConfidence(0.8)

    val results = ar.run(freqItemSets)

    results.collect.foreach(rule =>
      println(
        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent.mkString("[", ",", "]")
          + " , " + rule.confidence))

  }
}