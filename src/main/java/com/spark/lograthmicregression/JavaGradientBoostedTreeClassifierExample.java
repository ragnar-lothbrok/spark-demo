package com.spark.lograthmicregression;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
//$example on$
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.GBTClassificationModel;
import org.apache.spark.ml.classification.GBTClassifier;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
//$example off$
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

public class JavaGradientBoostedTreeClassifierExample {
	public static void main(String[] args) {
		final SparkConf sparkConf = new SparkConf().setAppName("Click Analysis").setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		SQLContext sqlContext = new SQLContext(javaSparkContext);

		// Load and parse the data file, converting it to a DataFrame.
		DataFrame data = sqlContext.read().format("libsvm").option("header", "false").load("/home/raghunandangupta/Downloads/splits/sample_libsvm_data.txt");

		data.printSchema();

		// Index labels, adding metadata to the label column. Fit on whole
		// dataset to include all labels in index.
		StringIndexerModel labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(data);
		// Automatically identify categorical features, and index them.
		// Set maxCategories so features with > 4 distinct values are treated as
		// continuous.
		VectorIndexerModel featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data);

		data.printSchema();

		// Split the data into training and test sets (30% held out for testing)
		DataFrame[] splits = data.randomSplit(new double[] { 0.7, 0.3 });
		DataFrame trainingData = splits[0];
		DataFrame testData = splits[1];

		
		testData.printSchema();
		// Train a GBT model.
		GBTClassifier gbt = new GBTClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setMaxIter(10);

		// Convert indexed labels back to original labels.
		IndexToString labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels());

		// Chain indexers and GBT in a Pipeline.
		Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] { labelIndexer, featureIndexer, gbt, labelConverter });

		// Train model. This also runs the indexers.
		PipelineModel model = pipeline.fit(trainingData);

		// Make predictions.
		DataFrame predictions = model.transform(testData);

		predictions.printSchema();

		// Select example rows to display.
		predictions.select("predictedLabel", "label", "features").show(50);

		predictions = predictions.select("predictedLabel", "label");

		JavaRDD<Tuple2<Object, Object>> predictionAndLabels = predictions.javaRDD().map(new Function<Row, Tuple2<Object, Object>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Object, Object> call(Row v1) throws Exception {
				return new Tuple2<Object, Object>(Double.parseDouble(v1.get(0).toString()), Double.parseDouble(v1.get(1).toString()));
			}
		});

		// Get evaluation metrics.
		MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());

		// Confusion matrix
		Matrix confusion = metrics.confusionMatrix();
		System.out.println("Confusion matrix: \n" + confusion);

		GBTClassificationModel gbtModel = (GBTClassificationModel) (model.stages()[2]);
		System.out.println("Learned classification GBT model:\n" + gbtModel.toDebugString());

	}
}
