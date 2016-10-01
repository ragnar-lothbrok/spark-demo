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

//https://acadgild.com/blog/spark-sql-use-case-machine-sensor-data-analysis/
public class SensorDataAnalytics {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("Temprature Difference");

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		SQLContext sqlContext = new SQLContext(javaSparkContext);

		DataFrame bulidingDataFrame = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true")
				.load("/home/raghunandangupta/gitPro/spark-demo/inputfiles/building.csv");

		DataFrame tempDataFrame = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true")
				.load("/home/raghunandangupta/gitPro/spark-demo/inputfiles/HVAC.csv");

		sqlContext.registerDataFrameAsTable(bulidingDataFrame, "building");

		sqlContext.registerDataFrameAsTable(tempDataFrame, "temprature");

		DataFrame joinedDataFrame = sqlContext
				.sql("select b.Country,t.ActualTemp,t.TargetTemp from building b join temprature t on t.BuildingID=b.BuildingID");

		JavaPairRDD<Integer, String> rdd = joinedDataFrame.toJavaRDD().map(new Function<Row, Tuple2<String, Integer>>() {
			private static final long serialVersionUID = -6133165404841203143L;

			@Override
			public Tuple2<String, Integer> call(Row v1) throws Exception {
				return new Tuple2<String, Integer>(v1.getString(0), Math.abs(v1.getInt(1) - v1.getInt(2)) > 5 ? 1 : 0);
			}
		}).filter(new Function<Tuple2<String, Integer>, Boolean>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, Integer> v1) throws Exception {
				return v1._2.intValue() == 1;
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
