package com.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

//https://acadgild.com/blog/spark-use-case-weather-data-analysis/
public class WeatherDataAnalytics {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("Weather Data Analysis");

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		JavaPairRDD<String, Double> rdd = javaSparkContext.textFile("/home/raghunandangupta/gitPro/spark-demo/inputfiles/Temperature.csv")
				.map(new Function<String, Tuple2<String, Double>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Double> call(String v1) throws Exception {
						String splits[] = v1.split(",");
						return new Tuple2<String, Double>(splits[0], (Double.parseDouble(splits[3]) * 0.1 * (9.0 / 5.0)) + 32.0);
					}
				}).mapToPair(new PairFunction<Tuple2<String, Double>, String, Double>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Double> call(Tuple2<String, Double> t) throws Exception {
						return new Tuple2<String, Double>(t._1, t._2);
					}
				}).reduceByKey(new Function2<Double, Double, Double>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(Double v1, Double v2) throws Exception {
						return Math.min(v1, v2);
					}
				});

		rdd.sortByKey(true).repartition(1).saveAsTextFile("/tmp/datas");

	}
}
