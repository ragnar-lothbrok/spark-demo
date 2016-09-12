package com.spark.ml.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.google.gson.Gson;
import com.spark.ml.demo.AdServerProductRequest;

public class CSVToJson {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("JavaRandomForestClassification");
		sparkConf.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> rdd = sc.textFile("/home/raghunandangupta/Downloads/splits/test_file");
		
		String firstLine = rdd.first();
		
		JavaRDD<AdServerProductRequest> records = rdd.filter(line -> line != firstLine)
				.map(new Function<String, AdServerProductRequest>() {
					private static final long serialVersionUID = 1L;

					public AdServerProductRequest call(String line) throws Exception {
						String[] fields = line.split(",");
						if(fields[0].equals("id")){
							return null;
						}
						AdServerProductRequest sd = new AdServerProductRequest(fields[0], fields[1], fields[2], fields[3], fields[4],
								fields[5], fields[6], fields[7], fields[8], fields[9], fields[10], fields[11], fields[12], fields[13], fields[14],
								fields[15], fields[16], fields[17], fields[18], fields[19], fields[20], fields[21], fields[22], fields[23]);
						return sd;
					}
				});
		System.out.println(new Gson().toJson(records.collect().subList(0, 300)));
		sc.close();
	}
}
