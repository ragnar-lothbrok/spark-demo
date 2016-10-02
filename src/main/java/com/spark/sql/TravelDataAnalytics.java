package com.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;

//https://acadgild.com/blog/spark-use-case-travel-data-analysis/
public class TravelDataAnalytics {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("Travel Data Analysis");

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		JavaRDD<Tuple3<String, String, String>> rdd = javaSparkContext.textFile("/home/raghunandangupta/gitPro/spark-demo/inputfiles/TravelData.txt")
				.map(new Function<String, Tuple3<String, String, String>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple3<String, String, String> call(String v1) throws Exception {
						String splits[] = v1.split("\t");
						return new Tuple3<String, String, String>(splits[2], splits[1], splits[3]);
					}
				});
		
		findTopSourceStation(rdd);
		findTopDestinationStation(rdd);
		findTopSourceRevenueStation(rdd);

	}

	private static void findTopSourceRevenueStation(JavaRDD<Tuple3<String, String, String>> rdd) {
		JavaPairRDD<Integer, String> resultantRdd = rdd.filter(new Function<Tuple3<String,String,String>, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple3<String, String, String> v1) throws Exception {
				return v1._3().equals("1");
			}
		}).mapToPair(new PairFunction<Tuple3<String,String,String>, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple3<String, String, String> t) throws Exception {
				return new Tuple2<String, Integer>(t._1(), Integer.parseInt(t._3()));
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+v2;
			}
		}).mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
				return new Tuple2<Integer, String>(t._2,t._1);
			}
		}).sortByKey(false);
		
		resultantRdd.repartition(1).saveAsTextFile("/tmp/data3");
	}

	private static void findTopDestinationStation(JavaRDD<Tuple3<String, String, String>> rdd) {
		JavaPairRDD<Integer, String> resultantRdd = rdd.mapToPair(new PairFunction<Tuple3<String,String,String>, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple3<String, String, String> t) throws Exception {
				return new Tuple2<String, Integer>(t._2(), 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+v2;
			}
		}).mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
				return new Tuple2<Integer, String>(t._2,t._1);
			}
		}).sortByKey(false);
		
		resultantRdd.repartition(1).saveAsTextFile("/tmp/data1");
	}

	private static void findTopSourceStation(JavaRDD<Tuple3<String, String, String>> rdd) {
		JavaPairRDD<Integer, String> resultantRdd = rdd.mapToPair(new PairFunction<Tuple3<String,String,String>, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple3<String, String, String> t) throws Exception {
				return new Tuple2<String, Integer>(t._1(), 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+v2;
			}
		}).mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
				return new Tuple2<Integer, String>(t._2,t._1);
			}
		}).sortByKey(false);
		
		resultantRdd.repartition(1).saveAsTextFile("/tmp/data2");
	}
}
