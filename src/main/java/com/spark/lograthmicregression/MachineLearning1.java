package com.spark.lograthmicregression;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import scala.Tuple2;

//https://www.hackerearth.com/notes/samarthbhargav/logistic-regression-in-apache-spark/
public class MachineLearning1 {

	@SuppressWarnings("resource")
	public static void main(String[] args) {

		final SparkConf sparkConf = new SparkConf().setAppName("Machine Learning").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<LabeledPoint>[] dataSets = sc.textFile("Qualitative_Bankruptcy.data.txt").map(x -> x.split(","))
				.map(x -> new LabeledPoint(getDoubleValue(x[6]), Vectors.dense(new double[] { getDoubleValue(x[0]), getDoubleValue(x[1]),
						getDoubleValue(x[2]), getDoubleValue(x[3]), getDoubleValue(x[4]), getDoubleValue(x[5]) })))
				.randomSplit(new double[] { 0.6, 0.4 }, 11l);

		LogisticRegressionModel model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(dataSets[0].rdd());

		JavaRDD<Tuple2<Double, Double>> predicted = dataSets[1].map(lp -> new Tuple2<Double, Double>(lp.label(), model.predict(lp.features())));

		float filterPerc = predicted.filter(data -> data._1.doubleValue() == data._2.doubleValue()).count() * 1.0f / dataSets[1].count();

		System.out.println(filterPerc);
	}

	private static double getDoubleValue(String str) {
		switch (str) {
		case "P":
			return 3.0;
		case "A":
			return 2.0;
		case "N":
			return 1.0;
		case "NB":
			return 1.0;
		case "B":
			return 0.0;
		}
		return 0.0;
	}
}
