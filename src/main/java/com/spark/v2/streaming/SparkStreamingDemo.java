package com.spark.v2.streaming;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class SparkStreamingDemo {
	public static void main(String[] args) {

		Logger.getLogger("org").setLevel(Level.ERROR);
		Logger.getLogger("akka").setLevel(Level.ERROR);

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("V2 Maestros");

		JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(3));

		JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream("localhost", 9000);

		lines.print();
		// Split each line into words
		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator call(String x) {
				return Arrays.asList(x.split(" ")).iterator();
			}
		});

		// Count each word in each batch
		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String x) {
				return new Tuple2<String, Integer>(x, 1);
			}
		});

		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer x, Integer y) {
				return x + y;
			}
		});

		wordCounts.print();

		JavaPairDStream<String, Integer> windowedWordCounts = pairs
				.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer x, Integer y) {
						return x + y;
					}
				}, Durations.seconds(3));

		windowedWordCounts.print();

		System.out.println("Starting Streaming...");
		streamingContext.start();

		try {
			streamingContext.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		streamingContext.close();

	}
}
