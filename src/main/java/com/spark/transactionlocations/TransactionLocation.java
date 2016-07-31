package com.spark.transactionlocations;

import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import com.google.common.base.Optional;

import scala.Tuple2;

//http://beekeeperdata.com/posts/hadoop/2015/12/28/spark-java-tutorial.html
public class TransactionLocation {

	public static void main(String[] args) {

		final SparkConf sparkConf = new SparkConf().setAppName("Transactions Location").setMaster("local");

		try (JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf)) {
			final String firstLine = javaSparkContext.textFile("/tmp/user").first();
			
			JavaPairRDD<Integer, String> customerPairs = javaSparkContext.textFile("/tmp/user").filter((x) -> !x.equals(firstLine))
					.map((x) -> x.split(",")).mapToPair((x) -> new Tuple2<Integer, String>(Integer.parseInt(x[0]), x[3]));

			final String firstLineT = javaSparkContext.textFile("/tmp/transactions").first();
			JavaPairRDD<Integer, Integer> transactionPairs = javaSparkContext.textFile("/tmp/transactions").filter((x) -> !x.equals(firstLineT))
					.map((x) -> x.split(",")).mapToPair((x) -> new Tuple2<Integer, Integer>(Integer.parseInt(x[2]), Integer.parseInt(x[1])));

			
			Map<Integer,Object> map = transactionPairs.leftOuterJoin(customerPairs).values().distinct().mapToPair(new PairFunction<Tuple2<Integer,Optional<String>>, Integer, String	>() {
				@Override
				public Tuple2<Integer, String> call(Tuple2<Integer, Optional<String>> t) throws Exception {
					return new Tuple2<Integer, String>(t._1, t._2.get());
				}
			}).countByKey();
			
			System.out.println(map);
		}
	}
}
