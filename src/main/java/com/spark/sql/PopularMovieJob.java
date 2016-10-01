package com.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple4;

//home/raghunandangupta/Downloads/popular_movies.txt
public class PopularMovieJob {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("Temprature Difference");

		@SuppressWarnings("resource")
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		JavaPairRDD<Integer, String> rdd = javaSparkContext.textFile("/home/raghunandangupta/gitPro/spark-demo/inputfiles/popular_movies.txt")
				.map(new Function<String, Tuple4<String, String, Integer, Long>>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Tuple4<String, String, Integer, Long> call(String v1) throws Exception {
						String splits[] = v1.split("\t");
						return new Tuple4<>(splits[0], splits[1],1/* Integer.parseInt(splits[2])*/, Long.parseLong(splits[3]));
					}
				}).map(new Function<Tuple4<String, String, Integer, Long>, Tuple2<String, Integer>>() {
					private static final long serialVersionUID = -6133165404841203143L;

					@Override
					public Tuple2<String, Integer> call(Tuple4<String, String, Integer, Long> v1) throws Exception {
						return new Tuple2<String, Integer>(v1._2(), v1._3());
					}
				}).mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(Tuple2<String, Integer> t) throws Exception {
						return new Tuple2<String, Integer>(t._1, t._2);
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
				}).mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
						return new Tuple2<Integer, String>(t._2, t._1);
					}
				});

		rdd.sortByKey(true).repartition(1).saveAsTextFile("/tmp/datas");

	}
}
