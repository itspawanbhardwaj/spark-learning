package com.spark.v2.training;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkActions {

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

		// SUM
		System.out.println("sum is: " + collData.reduce((x, y) -> x + y));
		int sumValue = collData.reduce(new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer x, Integer y) throws Exception {
				// TODO Auto-generated method stub
				return x + y;
			}
		});

		System.out.println("sum is: " + sumValue);

		// finding shortest no

		System.out.println("min is: " + collData.reduce((x, y) -> x < y ? x : y));

		int minValue = collData.reduce(new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer x, Integer y) throws Exception {
				// TODO Auto-generated method stub
				return x < y ? x : y;
			}
		});
		System.out.println(minValue);

		// shortest string in file
		JavaRDD<String> autoAllData = spContext.textFile("src/main/resources/data/auto-data.csv");
		String header = autoAllData.first();
		JavaRDD<String> rows = autoAllData.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String str) {
				return !str.equals(header);
			}
		});
		String shortString = rows.reduce(new Function2<String, String, String>() {

			@Override
			public String call(String x, String y) throws Exception {
				// TODO Auto-generated method stub
				return x.length() < y.length() ? x : y;
			}
		});

		System.out.println("shortest string is: " + shortString);

		// key value pair RDDs

		// JavaRDD<Tuple2<String, Integer[]>> autoKV = rows.map(new
		// Function<String, Tuple2<String, Integer[]>>() {
		// @Override
		// public Tuple2<String, Integer[]> call(String str) {
		//
		// String[] attList = str.split("\\,");
		// Integer[] hpVal = { (attList[7].equals("HP") ? 0 :
		// Integer.valueOf(attList[7])), 1 };
		// return new Tuple2<String, Integer[]>(attList[0], hpVal);
		// }
		// });

		JavaPairRDD<String, Integer[]> autoKV = rows.mapToPair(new getKV());

		System.out.println("KV RDD Demo - raw tuples :");

		for (Tuple2<String, Integer[]> tuple : autoKV.take(5)) {
			System.out.println(tuple._1 + " - " + tuple._2[0] + " - " + tuple._2()[1]);
		}

		// Find summarize total HP and total Count
		JavaPairRDD<String, Integer[]> autoSumKV = autoKV.reduceByKey(new computeTotalHP());

		System.out.println("KV RDD Demo - raw tuples after summerizing :");

		for (Tuple2<String, Integer[]> tuple : autoSumKV.take(5)) {
			System.out.println(tuple._1 + " - " + tuple._2[0] + " - " + tuple._2()[1]);
		}

		System.out.println("Finding avg using mapValues function");

		JavaPairRDD<String, Integer> autoAvgKV = autoSumKV.mapValues(x -> x[0] / x[1]);
		for (Tuple2<String, Integer> tuple : autoAvgKV.take(5)) {
			System.out.println(tuple._1 + " - " + tuple._2);
		}

		// partitioning
		System.out.println("No of partitions in autoAvgKV: " + autoAvgKV.getNumPartitions());

	}

}

class getKV implements PairFunction<String, String, Integer[]> {

	@Override
	public Tuple2<String, Integer[]> call(String arg0) throws Exception {

		String[] attList = arg0.split(",");
		// Handle header line
		Integer[] hpVal = { (attList[7].equals("HP") ? 0 : Integer.valueOf(attList[7])), 1 };
		return new Tuple2<String, Integer[]>(attList[0], hpVal);
	}

}

class computeTotalHP implements Function2<Integer[], Integer[], Integer[]> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8013459029031407606L;

	@Override
	public Integer[] call(Integer[] arg0, Integer[] arg1) throws Exception {
		Integer[] result = { arg0[0] + arg0[1], arg1[0] + arg1[1] };
		return result;
	}

}
