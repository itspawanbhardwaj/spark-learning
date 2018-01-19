package com.spark.v2.training;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

public class Variables {

	public static void main(String[] args) {
		SparkDriver.initSpark("Spark Operations", "local[2]");
		JavaSparkContext spark = SparkDriver.getContext();

		LongAccumulator longAcc = spark.sc().longAccumulator();

		Broadcast<String> text = spark.broadcast("sedan");

		JavaRDD<String> autoAllData = spark.textFile("src/main/resources/data/auto-data.csv");
		String header = autoAllData.first();
		JavaRDD<String> rows = autoAllData.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String str) {
				return !str.equals(header);
			}
		});

		JavaRDD rdd = rows.map(new Function<String, String>() {
			@Override
			public String call(String str) {
				if (str.contains(text.value()))
					longAcc.add(1);
				return str;
			}
		});

		rdd.count();
		System.out.println("longAcc: " + longAcc);
	}
}
