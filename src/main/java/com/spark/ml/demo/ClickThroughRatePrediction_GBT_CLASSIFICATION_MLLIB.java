package com.spark.ml.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.GradientBoostedTrees;
import org.apache.spark.mllib.tree.configuration.BoostingStrategy;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

public class ClickThroughRatePrediction_GBT_CLASSIFICATION_MLLIB {

	private static final String ModelType = "classification";

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("JavaRandomForestClassification");
		sparkConf.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		// Load and parse the data file.
		String datapath = "/home/raghunandangupta/Downloads/train_1.data";
		JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc.sc(), datapath).toJavaRDD();

		/**
		 * Column count in train data set
		 */
		Long maxBin = 0l;
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

		if ("classification".equalsIgnoreCase(ModelType)) {
			BoostingStrategy boostingStrategy = BoostingStrategy.defaultParams("Classification");
			boostingStrategy.setNumIterations(50);
			boostingStrategy.setLearningRate(0.15);
			boostingStrategy.setValidationTol(5);
			boostingStrategy.getTreeStrategy().setMaxDepth(7);
			boostingStrategy.getTreeStrategy().setMaxBins(maxBin.intValue());
			boostingStrategy.getTreeStrategy().setCheckpointInterval(10);

			final GradientBoostedTreesModel model = GradientBoostedTrees.train(trainingData, boostingStrategy);
			JavaPairRDD<Double, Double> predictionAndLabel = testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<Double, Double> call(LabeledPoint p) {
					return new Tuple2<>(model.predict(p.features()), p.label());
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
			System.out.println("Learned classification GBT model:\n" + model.toDebugString());

			// Save and load model
			model.save(sc.sc(), "target/tmp/myGradientBoostingClassificationModel");
			GradientBoostedTreesModel sameModel = GradientBoostedTreesModel.load(sc.sc(), "target/tmp/myGradientBoostingClassificationModel");
			System.out.println("Model retrieved : " + sameModel);
			
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
			BoostingStrategy boostingStrategy = BoostingStrategy.defaultParams("Regression");
			boostingStrategy.setNumIterations(100);
			boostingStrategy.setLearningRate(0.15);
			boostingStrategy.setValidationTol(5);
			boostingStrategy.getTreeStrategy().setMaxDepth(7);
			boostingStrategy.getTreeStrategy().setMaxBins(maxBin.intValue());
			boostingStrategy.getTreeStrategy().setCheckpointInterval(10);

			final GradientBoostedTreesModel model = GradientBoostedTrees.train(trainingData, boostingStrategy);

			// Evaluate model on training instances and compute training error
			JavaPairRDD<Double, Double> predictionAndLabel = data.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<Double, Double> call(LabeledPoint p) {
					return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
				}
			});
			Double trainMSE = predictionAndLabel.map(new Function<Tuple2<Double, Double>, Double>() {

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
			}) / data.count();
			System.out.println("Training Mean Squared Error: " + trainMSE);
			System.out.println("Learned regression tree model:\n" + model);
		}

		sc.close();
	}
}
