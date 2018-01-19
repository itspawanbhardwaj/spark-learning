package com.spark.v2.training;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkSampleProgram {

	public static void main(String args[]) {
		System.out.println("Lets rock!!!");
		String appName = "Spark Training";
		String sparkMaster = "local";

		JavaSparkContext sc = null;
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(
				sparkMaster);
		sc = new JavaSparkContext(conf);
		JavaRDD<String> tweetsRDD = sc
				.textFile("src/main/resources/data/movietweets.csv");

		for (String s : tweetsRDD.take(5)) {
			System.out.println(s);
		}
		System.out.println("total tweets in the file: " + tweetsRDD.count());
		
		sc.close();
	}
}
