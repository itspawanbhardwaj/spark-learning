package com.spark.v2.training;

import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.RowFactory;

public class DatasetDemoExample {

	public static void main(String[] args) {
		SparkDriver.initSpark("Spark Operations", "local[2]");
		JavaSparkContext spark = SparkDriver.getContext();

		RowFactory.create(1, "USA");
	}
}
