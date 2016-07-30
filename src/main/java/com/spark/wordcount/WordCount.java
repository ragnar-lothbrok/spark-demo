package com.spark.wordcount;

import java.util.Arrays;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount<T> {

	public static void main(String[] args) {

		final SparkConf sparkConf = new SparkConf().setAppName("Word Count").setMaster("local");

		try (final JavaSparkContext jSC = new JavaSparkContext(sparkConf)) {

			Map<String, Object> map = jSC.textFile(args[0]).flatMap((x) -> Arrays.asList(x.split(" ")))
					.mapToPair((x) -> new Tuple2<String, Integer>(x, 1)).countByKey();
			System.out.println(map);
		}
	}
}
