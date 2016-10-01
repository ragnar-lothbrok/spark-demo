package com.spark.streams;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;


//nc -lk 9999
public class WordStream {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("WordCount Streaming").setMaster("local[2]");

		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, new Duration(2000));

		JavaReceiverInputDStream<String> stream = streamingContext.socketTextStream("localhost", 9999, StorageLevel.DISK_ONLY());

		JavaDStream<String> words = stream.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(String x) {
				return Arrays.asList(x.split(" "));
			}
		});

		// Count each word in each batch
		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});

		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		// Print the first ten elements of each RDD generated in this DStream to
		// the console
		wordCounts.print();

		streamingContext.start();
		streamingContext.awaitTermination();
	}
}
