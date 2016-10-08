package com.spark.sql;

import java.util.function.ToDoubleFunction;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

//https://acadgild.com/blog/analyzing-new-york-crime-data-using-sparksql/
public class CrimeDataAnalytics {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("Crime Applciation");

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		SQLContext sqlContext = new SQLContext(javaSparkContext);

		DataFrame crimeDataFrame = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "false")
				.load("/home/raghunandangupta/gitPro/spark-demo/inputfiles/Crime_dataset.csv");
		crimeDataFrame.registerTempTable("crimes");
		DataFrame crimesUnderFBI = sqlContext.sql("select C14,count(C14) from crimes group by C14");
		crimesUnderFBI.show();

		crimesUnderFBI = sqlContext.sql("select count(*) as count from crimes where C5 ='NARCOTICS' and C17 = 2015 ");
		crimesUnderFBI.show();
		
		crimesUnderFBI = sqlContext.sql("select C11 ,count(*) as count from crimes where C5 ='THEFT' and arrest = 'true' group by C11 ");
		crimesUnderFBI.show();
		
		double[] data = crimeDataFrame.select("C0").javaRDD().map(new Function<Row, Double>() {
			@Override
			public Double call(Row v1) throws Exception {
				return ((double)v1.getInt(0));
			}
		}).collect().stream().mapToDouble(new ToDoubleFunction<Double>() {

			@Override
			public double applyAsDouble(Double value) {
				return value;
			}
		}).toArray();
		
		DescriptiveStatistics DescriptiveStatistics = new DescriptiveStatistics(data);
		double meanQ1 = DescriptiveStatistics.getPercentile(25);
		double mean = DescriptiveStatistics.getPercentile(50);
		double meanQ3 = DescriptiveStatistics.getPercentile(75);
		double meanQ2 = meanQ3 - meanQ1;
		System.out.println(meanQ1+" "+meanQ2+" "+meanQ3+" "+mean);
		System.out.println();
	}
}
