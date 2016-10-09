package com.spark.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class ReduceRDDs {

	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("Daily Show Movie Analysis");

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		JavaPairRDD<String, List<String>> rdd = javaSparkContext.textFile("/tmp/mathsetdata.dat").filter(new Function<String, Boolean>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(String v1) throws Exception {
				String split[] = v1.split(" ");
				return split[0].equals("suman") || split[0].equals("anshu") || split[0].equals("neeraj");
			}
		}).mapToPair(new PairFunction<String, String, List<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, List<String>> call(String t) throws Exception {
				String split[] = t.split(" ");
				List<String> list = new ArrayList<String>();
				list.add(split[1].trim());
				return new Tuple2<String, List<String>>(split[0].trim(), list);
			}
		}).reduceByKey(new Function2<List<String>, List<String>, List<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public List<String> call(List<String> v1, List<String> v2) throws Exception {
				List<String> list = new ArrayList<String>();
				list.addAll(v1);
				list.addAll(v2);
				return list;
			}
		});

		Tuple2<String, Integer> rdds = rdd.filter(new Function<Tuple2<String, List<String>>, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, List<String>> v1) throws Exception {
				return v1._1.equals("suman");
			}
		}).map(new Function<Tuple2<String, List<String>>, Tuple2<String, Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple2<String, List<String>> v1) throws Exception {
				Integer sum = 0;
				for (String str : v1._2) {
					sum += Integer.parseInt(str);
				}
				return new Tuple2<String, Integer>(v1._1, sum);
			}
		}).collect().get(0);

		System.out.println(rdds);

		System.out.println(rdd.collect());
	}
}
