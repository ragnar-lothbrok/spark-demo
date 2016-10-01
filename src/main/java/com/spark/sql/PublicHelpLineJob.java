package com.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

//https://acadgild.com/blog/spark-sql-use-case-911-emergency-helpline-number-data-analysis/
public class PublicHelpLineJob {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("HelpLine Applciation");

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		SQLContext sqlContext = new SQLContext(javaSparkContext);

		DataFrame zipCodeDataFrame = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true")
				.load("/home/raghunandangupta/gitPro/spark-demo/inputfiles/zipcode.csv");

		DataFrame emergencyDataFrame = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true")
				.load("/home/raghunandangupta/gitPro/spark-demo/inputfiles/911.csv");

		System.out.println(" " + zipCodeDataFrame.count() + " " + emergencyDataFrame.count());

		sqlContext.registerDataFrameAsTable(emergencyDataFrame, "emergency");

		sqlContext.registerDataFrameAsTable(zipCodeDataFrame, "zipcode");

		DataFrame joinedDataFrame = sqlContext.sql("select e.title, z.city,z.state from emergency e join zipcode z on e.zip = z.zip");

		query1(joinedDataFrame);
		query2(joinedDataFrame);
	}
	
	private static void query2(DataFrame joinedDataFrame) {
		JavaPairRDD<String,Integer> rdd = joinedDataFrame.toJavaRDD().map(new Function<Row, String>() {
			private static final long serialVersionUID = -6133165404841203143L;

			@Override
			public String call(Row v1) throws Exception {
				return v1.getString(0).split(":")[0] + "-->" + v1.getString(1);
			}
		}).mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		rdd.repartition(1).saveAsTextFile("/tmp/dataa");
	}

	private static void query1(DataFrame joinedDataFrame) {
		JavaPairRDD<String,Integer> rdd = joinedDataFrame.toJavaRDD().map(new Function<Row, String>() {
			private static final long serialVersionUID = -6133165404841203143L;

			@Override
			public String call(Row v1) throws Exception {
				return v1.getString(0).split(":")[0] + "-->" + v1.getString(2);
			}
		}).mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		rdd.repartition(1).saveAsTextFile("/tmp/data");
	}
}
