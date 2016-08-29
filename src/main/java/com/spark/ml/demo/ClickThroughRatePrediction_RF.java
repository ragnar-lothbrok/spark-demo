package com.spark.ml.demo;

import java.util.HashMap;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

public class ClickThroughRatePrediction_RF {

	private static final String ModelType = "classfication";

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("JavaRandomForestClassification");
		sparkConf.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		// Load and parse the data file.
		String datapath = "/home/raghunandangupta/Downloads/abcdaa";
		JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc.sc(), datapath).toJavaRDD();

		/**
		 * This will find count of max number of distinct category. Will be used
		 * in MaxBin
		 */
		Long maxBin = 0l;
		/**
		 * This will tell us how many column each row has
		 */

		for (double[] dob : data.collect().stream().map(lp -> ((org.apache.spark.mllib.linalg.SparseVector) lp.features()).values())
				.collect(Collectors.toList())) {
			System.out.println(dob.length);
		}

		/**
		 * Column count in train data set
		 */
		int length = ((org.apache.spark.mllib.linalg.SparseVector) data.collect().get(0).features()).values().length;
		for (int i = 0; i < length; i++) {
			final int index = i;
			try {
				long temp = data.collect().stream().map(lp -> ((org.apache.spark.mllib.linalg.SparseVector) lp.features()).values())
						.map(lp -> lp[index]).distinct().count();
				if (maxBin < temp) {
					maxBin = temp;
				}
			} catch (Exception exception) {
				System.out.println("Exception occured " + exception.getMessage());
			}
		}

		// Split the data into training and test sets (30% held out for testing)
		JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[] { 0.7, 0.3 });
		JavaRDD<LabeledPoint> trainingData = splits[0];
		JavaRDD<LabeledPoint> testData = splits[1];

		// Train a RandomForest model.
		// Empty categoricalFeaturesInfo indicates all features are continuous.
		Integer numClasses = 2;
		HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
		Integer numTrees = 20; // Use more in practice.
		String featureSubsetStrategy = "auto"; // Let the algorithm choose.
		String impurity = "gini";
		Integer maxDepth = 8;
		Integer seed = 12345;

		if ("classfication".equalsIgnoreCase(ModelType)) {
			final RandomForestModel model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, numTrees,
					featureSubsetStrategy, impurity, maxDepth, maxBin.intValue(), seed);

			// Evaluate model on test instances and compute test error
			JavaPairRDD<Double, Double> predictionAndLabel = testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<Double, Double> call(LabeledPoint p) {
					return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
				}
			});

			Double testErr = 1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Boolean call(Tuple2<Double, Double> pl) {
					return !pl._1().equals(pl._2());
				}
			}).count() / testData.count();
			System.out.println("Test Error: " + testErr);
			System.out.println("Learned classification forest model:\n" + model.toDebugString());

			JavaRDD<Tuple2<Object, Object>> predictions = predictionAndLabel.map(new Function<Tuple2<Double, Double>, Tuple2<Object, Object>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<Object, Object> call(Tuple2<Double, Double> v1) throws Exception {
					return new Tuple2<Object, Object>(v1._1, v1._2);
				}
			});

			// Get evaluation metrics.
			MulticlassMetrics metrics = new MulticlassMetrics(predictions.rdd());
			System.out.println(
					"Confusion matrix: \n" + metrics.confusionMatrix() + " Precision : " + metrics.precision() + " Recall : " + metrics.recall());
		} else {
			numTrees = 100;
			featureSubsetStrategy = "auto";
			impurity = "variance";
			maxDepth = 4;
			seed = 12345;

			final RandomForestModel model = RandomForest.trainRegressor(trainingData, categoricalFeaturesInfo, numTrees, featureSubsetStrategy,
					impurity, maxDepth, maxBin.intValue(), seed);

			// Evaluate model on test instances and compute test error
			JavaPairRDD<Double, Double> predictionAndLabel = testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<Double, Double> call(LabeledPoint p) {
					return new Tuple2<>(model.predict(p.features()), p.label());
				}
			});
			Double testMSE = predictionAndLabel.map(new Function<Tuple2<Double, Double>, Double>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Double call(Tuple2<Double, Double> pl) {
					Double diff = pl._1() - pl._2();
					return diff * diff;
				}
			}).reduce(new Function2<Double, Double, Double>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Double call(Double a, Double b) {
					return a + b;
				}
			}) / testData.count();
			System.out.println("Test Mean Squared Error: " + testMSE);
			System.out.println("Learned regression forest model:\n" + model.toDebugString());

			// Save and load model
			model.save(sc.sc(), "target/tmp/myRandomForestRegressionModel");
			RandomForestModel sameModel = RandomForestModel.load(sc.sc(), "target/tmp/myRandomForestRegressionModel");
			System.out.println("Model retrieved : " + sameModel);
		}
		sc.close();
	}
}
