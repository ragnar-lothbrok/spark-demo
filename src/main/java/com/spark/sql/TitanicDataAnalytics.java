package com.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

//https://acadgild.com/blog/spark-use-case-titanic-data-analysis/
public class TitanicDataAnalytics {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("Travel Data Analysis");

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		findAverageMaleFemaleAge(javaSparkContext);

		JavaPairRDD<String, Integer> rdd = javaSparkContext.textFile("/home/raghunandangupta/gitPro/spark-demo/inputfiles/TitanicData.txt").mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				String splits[] = t.split(",");
				return new Tuple2<String, Integer>(splits[1]+" "+ splits[4] +" "+splits[6] +" "+splits[2],1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
		});
		
		rdd.repartition(1).saveAsTextFile("/tmp/data4");

	}

	private static void findAverageMaleFemaleAge(JavaSparkContext javaSparkContext) {
		JavaPairRDD<String, Double> rdd = javaSparkContext.textFile("/home/raghunandangupta/gitPro/spark-demo/inputfiles/TitanicData.txt")
				.filter(new Function<String, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(String v1) throws Exception {
						String splits[] = v1.split(",");
						return Integer.parseInt(splits[1]) == 1 && splits[5].matches(("\\d+"));
					}
				}).mapToPair(new PairFunction<String, String, Tuple2<Integer, Integer>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Tuple2<Integer, Integer>> call(String t) throws Exception {
						String splits[] = t.split(",");
						return new Tuple2<String, Tuple2<Integer, Integer>>(splits[4], new Tuple2<Integer, Integer>(Integer.parseInt(splits[5]), 1));
					}
				}).reduceByKey(new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
						return new Tuple2<Integer, Integer>(v1._1 + v2._1(), v1._2 + v2._2());
					}
				}).mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, Integer>>, String, Double>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Double> call(Tuple2<String, Tuple2<Integer, Integer>> t) throws Exception {
						return new Tuple2<String, Double>(t._1(), t._2._1() * 1.0 / t._2()._2());
					}
				});

		rdd.repartition(1).saveAsTextFile("/tmp/data3");
	}

}
