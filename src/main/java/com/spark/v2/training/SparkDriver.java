package com.spark.v2.training;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkDriver {

	private static JavaSparkContext spContext = null;
	private static SparkSession sparkSession = null;

	static void initSpark(String appName, String sparkMaster) {

		if (spContext == null) {
			// Setup Spark configuration
			SparkConf conf = new SparkConf().setAppName(appName).setMaster(
					sparkMaster);

			// Make sure you download the winutils binaries into this directory
			// from
			// https://github.com/srccodes/hadoop-common-2.2.0-bin/archive/master.zip
			System.setProperty("hadoop.home.dir", "c:\\spark\\winutils\\");

			// Create Spark Context from configuration
			spContext = new JavaSparkContext(conf);

			sparkSession = SparkSession
					.builder()
					.appName(appName)
					.master(sparkMaster)
					/* .config("spark.sql.warehouse.dir", tempDir) */.getOrCreate();

		}

	}

	public static JavaSparkContext getContext() {
		return spContext;
	}

	public static SparkSession getSession() {
		return sparkSession;
	}
	
	public static JavaRDD<Integer> getCollData() {
		JavaSparkContext spContext = getContext();
		List<Integer> data = Arrays.asList(3,6,3,4,8);
		JavaRDD<Integer> collData = spContext.parallelize(data);
		collData.cache();
		return collData;
	}
}
