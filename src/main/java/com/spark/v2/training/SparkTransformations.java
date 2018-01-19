package com.spark.v2.training;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

public class SparkTransformations {

	public static void main(String[] args) {
		System.out.println("lets rock!!");

		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);
		SparkDriver.initSpark("Spark Operations", "local[2]");
		JavaSparkContext spContext = SparkDriver.getContext();

		/*-------------------------------------------------------------------
		 * Loading and Storing Data
		 -------------------------------------------------------------------*/
		// create RDD from a list
		JavaRDD<Integer> collData = SparkDriver.getCollData();
		System.out.println("Total no of records: " + collData.count());

		// Create a RDD from a file
		JavaRDD<String> autoAllData = spContext.textFile("src/main/resources/data/auto-data.csv");
		autoAllData.cache();
		System.out.println("Total Records in Autodata : " + autoAllData.count());

		System.out.println("records");
		for (String rec : autoAllData.take(5)) {
			System.out.println(rec);
		}

		System.out.println("Storing RDD");
		// autoAllData
		// .saveAsTextFile("src/main/resources/data/auto-data-modified-stored.csv");

		// RDDs can also be converted back to Java Lists with collect(). Then
		// Java lists can be stored using any data sink and any technique
		// available in standard Java.
		List<String> autoList = autoAllData.collect();
		System.out.println(autoList);

		/*-------------------------------------------------------------------
		 * Spark Transformation
		 -------------------------------------------------------------------*/

		// Map Example: Change CSV to TSV for java 8
		System.out.println("spark Transformation: map");
		JavaRDD<String> tsvData = autoAllData.map(str -> str.replace(",", "\t"));
		for (String str : tsvData.take(5)) {
			System.out.println(str);
		}

		// Remove first header line

		String header = autoAllData.first();
		System.out.println("spark Transformations: filter ");
		JavaRDD<String> autoData = autoAllData.filter(str -> !str.equals(header));
		for (String str : autoData.take(5)) {
			System.out.println(str);
		}
		JavaRDD<String> toyotaData = autoData.filter(str -> str.contains("toyota"));

		System.out.println("spark Transformations: distinct ");

		for (String str : autoAllData.distinct().take(5)) {
			System.out.println(str);
		}

		System.out.println("Spark Transformations : flat map");
		JavaRDD<String> words = toyotaData.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String s) {
				return Arrays.asList(s.split(",")).iterator();
			}
		});

		for (String str : words.take(5)) {
			System.out.println(str);
		}
		System.out.println("no of words: " + words.count());

		// Using function for map
		JavaRDD<String> cleansedRDD = autoData.map(new cleanseRDD());
		System.out.println("Using Functions for Map : ");

		for (String str : cleansedRDD.take(5)) {
			System.out.println(str);
		}

		// Set operations

		JavaRDD<String> words1 = spContext.parallelize(Arrays.asList("Hello", "war", "peace"));
		JavaRDD<String> words2 = spContext.parallelize(Arrays.asList("war", "universe"));
		System.out.println("Union transformation");
		for (String str : words1.union(words2).take(5)) {
			System.out.println(str);
		}

		System.out.println("Intersaction transformation");
		for (String str : words1.intersection(words2).take(5)) {
			System.out.println(str);
		}
	}
}

/* class to cleanse the autoRDD */
class cleanseRDD implements Function<String, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public String call(String autoStr) throws Exception {

		// Splite into attributes
		String[] attList = autoStr.split(",");
		// Change number of doors to a number
		attList[3] = (attList[3].equals("two")) ? "2" : "4";
		// Change drive to upper case
		attList[5] = attList[5].toUpperCase();

		// Return modified string
		return Arrays.toString(attList);
	}

}
