package com.spark.sql;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

//https://acadgild.com/blog/spark-use-case-daily-show/
public class DailyShowMovieAnalysis {

	private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MM/dd/yy");

	public static void main(String[] args) throws ParseException {
		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("Daily Show Movie Analysis");

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
		
		JavaPairRDD<Integer, String> rdd = javaSparkContext.textFile("/home/raghunandangupta/gitPro/spark-demo/inputfiles/dialy_show_guests")
				.map(new Function<String, Tuple2<String, Date>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Date> call(String v1) throws Exception {
						String splits[] = v1.split(",");
						return new Tuple2<String, Date>(splits[1], simpleDateFormat.parse(splits[2]));
					}
				}).filter(new Function<Tuple2<String, Date>, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, Date> v1) throws Exception {
						return v1._2.before(simpleDateFormat.parse("6/11/99")) && v1._2().after(simpleDateFormat.parse("1/11/99"));
					}
				}).mapToPair(new PairFunction<Tuple2<String, Date>, String, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(Tuple2<String, Date> t) throws Exception {
						return new Tuple2<String, Integer>(t._1, 1);
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
