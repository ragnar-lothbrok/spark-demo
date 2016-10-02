package com.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

//https://acadgild.com/blog/spark-use-case-olympics-data-analysis/
public class OlympicDataAnalytics {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("Olympic Data Analysis");

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		findCountryWiseMedal(javaSparkContext, "Swimming");

		findYearWiseMedal(javaSparkContext);

		findCountryWiseMedal(javaSparkContext, null);
	}

	private static void findYearWiseMedal(JavaSparkContext javaSparkContext) {
		JavaPairRDD<String, Integer> rdd = javaSparkContext.textFile("/home/raghunandangupta/gitPro/spark-demo/inputfiles/olympix_data.csv")
				.filter(new Function<String, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(String v1) throws Exception {
						String splits[] = v1.split("\t");
						return splits[2].equalsIgnoreCase("India");
					}
				}).mapToPair(new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(String t) throws Exception {
						String splits[] = t.split("\t");
						return new Tuple2<String, Integer>(splits[3],
								Integer.parseInt(splits[6]) + Integer.parseInt(splits[7]) + Integer.parseInt(splits[8]));
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
				});
		rdd.repartition(1).saveAsTextFile("/tmp/data2");
	}

	private static void findCountryWiseMedal(JavaSparkContext javaSparkContext, final String sports) {
		JavaPairRDD<String, Integer> rdd = javaSparkContext.textFile("/home/raghunandangupta/gitPro/spark-demo/inputfiles/olympix_data.csv")
				.filter(new Function<String, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(String v1) throws Exception {
						String splits[] = v1.split("\t");
						return sports == null ? true : splits[5].equals(sports);
					}
				}).mapToPair(new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(String t) throws Exception {
						String splits[] = t.split("\t");
						return new Tuple2<String, Integer>(splits[2],
								Integer.parseInt(splits[6]) + Integer.parseInt(splits[7]) + Integer.parseInt(splits[8]));
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
				});
		if (sports == null) {
			rdd.repartition(1).saveAsTextFile("/tmp/data3");
		} else {
			rdd.repartition(1).saveAsTextFile("/tmp/data4");
		}
	}

}
