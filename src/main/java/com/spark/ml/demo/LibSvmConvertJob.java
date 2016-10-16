package com.spark.ml.demo;

import java.nio.charset.StandardCharsets;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;

import com.google.common.hash.Hashing;

public class LibSvmConvertJob {
	
	private static final String SPACE = " ";
	private static final String COLON = ":";

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("Libsvm Convertor");

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		SQLContext sqlContext = new SQLContext(javaSparkContext);

		DataFrame inputDF = sqlContext.read().format("com.databricks.spark.csv").option("header", "true")
				.load("/home/raghunandangupta/inputfiles/zipcode.csv");
		
		inputDF.printSchema();
		
		sqlContext.udf().register("convertToNull", (String v1) -> (v1.trim().length() > 0 ? v1.trim() : null), DataTypes.StringType);

		inputDF = inputDF.selectExpr("convertToNull(zip)","convertToNull(city)","convertToNull(state)","convertToNull(latitude)","convertToNull(longitude)","convertToNull(timezone)","convertToNull(dst)").na().drop();

		inputDF.javaRDD().map(new Function<Row, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String call(Row v1) throws Exception {
				StringBuilder sb = new StringBuilder();
				sb.append(hashCode(v1.getString(0))).append("\t")   //Resultant column
				.append("1"+COLON+hashCode(v1.getString(1))).append(SPACE)
				.append("2"+COLON+hashCode(v1.getString(2))).append(SPACE)
				.append("3"+COLON+hashCode(v1.getString(3))).append(SPACE)
				.append("4"+COLON+hashCode(v1.getString(4))).append(SPACE)
				.append("5"+COLON+hashCode(v1.getString(5))).append(SPACE)
				.append("6"+COLON+hashCode(v1.getString(6)));
				return sb.toString();
			}
			private String hashCode(String value) {
				return Math.abs(Hashing.murmur3_32().hashString(value, StandardCharsets.UTF_8).hashCode()) + "";
			}
		}).repartition(1).saveAsTextFile("/home/raghunandangupta/inputfiles/zipcode");

	}
}
