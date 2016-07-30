package com.spark.uberanalytics;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;

//https://acadgild.com/blog/spark-use-case-uber-data-analysis/
public class MaxTrip {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {

		final SparkConf sparkConf = new SparkConf().setAppName("Max Trip Count").setMaster("local");
		SimpleDateFormat sf = new SimpleDateFormat("MM/dd/yyyy");
		String days[] = new String[] { "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat" };
		try (final JavaSparkContext jSC = new JavaSparkContext(sparkConf)) {
			String line = jSC.textFile(args[0]).first();
			System.out.println("^^^" + jSC.textFile(args[0]).filter(x -> !x.equals(line)).map((x) -> x.split(","))
					.map((x) -> new Tuple3<>(x[0], days[sf.parse(x[1]).getDay()], Integer.parseInt(x[3])))
					.map(new Function<Tuple3<String, String, Integer>, Map<String, Integer>>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Map<String, Integer> call(Tuple3<String, String, Integer> arg0) throws Exception {
							Map<String, Integer> map = new HashMap<String, Integer>();
							map.put(arg0._1() + "" + arg0._2(), arg0._3());
							return map;
						}
					}).mapToPair(new PairFunction<Map<String, Integer>, String, Integer>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Tuple2<String, Integer> call(Map<String, Integer> arg0) throws Exception {
							String key = new ArrayList<String>(arg0.keySet()).get(0);
							return new Tuple2<String, Integer>(key, arg0.get(key));
						}
					}).reduceByKey(new Function2<Integer, Integer, Integer>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Integer call(Integer arg0, Integer arg1) throws Exception {
							return arg0 + arg1;
						}
					}).collect());
		}
	}
}
