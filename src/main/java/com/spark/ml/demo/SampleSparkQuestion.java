package com.spark.ml.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class SampleSparkQuestion {

	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("Libsvm Convertor");

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> rdd = javaSparkContext.textFile("/home/raghunandangupta/inputfiles/zipcode.csv");

		SQLContext sqlContext = new SQLContext(javaSparkContext);

		DataFrame inputDF = sqlContext.read().format("com.databricks.spark.csv").option("header", "true")
				.load("/home/raghunandangupta/inputfiles/zipcode.csv");
		
		inputDF.printSchema();
		
		
		
	}
}
