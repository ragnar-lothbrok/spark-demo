package com.spark.ml.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class WeatherAnalyserML {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("Java Decision Tree Classification");
		sparkConf.setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(javaSparkContext);
		
		final DataFrame inputDF = sqlContext.read().format("com.databricks.spark.csv").option("header", "true").load("src/main/resources/datatraining.txt");

		DataFrame filteredDF = inputDF.na().drop(inputDF.columns().length,inputDF.columns());
		
		System.out.println(filteredDF.count()+"======"+inputDF.count());

		// Create view and execute query to convert types as, by default, all
		// columns have string types
		inputDF.registerTempTable("weather_traning_data");

		final DataFrame modifiedDF = sqlContext.sql("SELECT cast(Temperature as float) Temperature, cast(Humidity as float) Humidity, "
				+ "cast(Light as float) Light, cast(CO2 as float) CO2, " + "cast(HumidityRatio as float) HumidityRatio, "
				+ "cast(Occupancy as int) Occupancy FROM weather_traning_data");

		modifiedDF.show(5);

		// Combine multiple input columns to a Vector using Vector Assembler
		// utility
		VectorAssembler vectorAssembler = new VectorAssembler()
				.setInputCols(new String[] { "Temperature", "Humidity", "Light", "CO2", "HumidityRatio" }).setOutputCol("features");

		DataFrame featuresData = vectorAssembler.transform(modifiedDF);

		// Indexing is done to improve the execution times as comparing indexes
		// is much cheaper than comparing strings/floats

		// Index labels, adding metadata to the label column (Occupancy). Fit on
		// whole dataset to include all labels in index.
		final StringIndexerModel labelIndexer = new StringIndexer().setInputCol("Occupancy").setOutputCol("indexedLabel").fit(featuresData);

		// Index features vector
		final VectorIndexerModel featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(featuresData);

		// Split the data into training and test sets (30% held out for
		// testing).
		DataFrame[] frames = featuresData.randomSplit(new double[] { 0.7, 0.3 });

		final DecisionTreeClassifier dt = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures");

		// Convert indexed labels back to original labels.
		final IndexToString labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedOccupancy")
				.setLabels(labelIndexer.labels());

		// Chain indexers and tree in a Pipeline.
		final Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] { labelIndexer, featureIndexer, dt, labelConverter });

		// Train model. This also runs the indexers.
		final PipelineModel model = pipeline.fit(frames[0]);

		// Make predictions.
		final DataFrame predictions = model.transform(frames[1]);

		// Select example rows to display.
		System.out.println("Example records with Predicted Occupancy as 0:");
		predictions.select("predictedOccupancy", "Occupancy", "features").where(predictions.col("predictedOccupancy").equalTo(0)).show(10);

		System.out.println("Example records with Predicted Occupancy as 1:");
		predictions.select("predictedOccupancy", "Occupancy", "features").where(predictions.col("predictedOccupancy").equalTo(1)).show(10);

		System.out.println("Example records with In-correct predictions:");
		predictions.select("predictedOccupancy", "Occupancy", "features")
				.where(predictions.col("predictedOccupancy").notEqual(predictions.col("Occupancy"))).show(10);
	}
}
