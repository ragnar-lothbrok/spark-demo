package com.spark.ml.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class LibSvmConvertJob {

	private static final String SPACE = " ";
	private static final String COLON = ":";

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("Libsvm Convertor");

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		SQLContext sqlContext = new SQLContext(javaSparkContext);

		DataFrame inputDF = sqlContext.read().format("com.databricks.spark.csv").option("header", "true")
				.load("/home/raghunandangupta/Downloads/soc_gen_data/train.csv");

		inputDF.printSchema();

		inputDF.javaRDD().map(new Function<Row, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Row v1) throws Exception {
				StringBuilder sb = new StringBuilder();
				sb.append(Integer.parseInt(v1.getString(101)) == -1 ? 0 : v1.getString(101)).append("\t"); // Resultant column
				for (int i = 0; i <= 100; i++) {
					sb.append((i+1) + COLON + v1.getString(0)).append(SPACE);
				}
				return sb.toString();
			}
		}).repartition(1).saveAsTextFile("/home/raghunandangupta/Downloads/soc_gen_data/train");

	}
}
