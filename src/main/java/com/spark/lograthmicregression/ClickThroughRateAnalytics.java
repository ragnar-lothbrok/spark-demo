package com.spark.lograthmicregression;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.GBTClassificationModel;
import org.apache.spark.ml.classification.GBTClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import com.google.common.collect.ImmutableMap;

import scala.collection.mutable.Seq;

//https://github.com/yu-iskw/click-through-rate-prediction
///https://www.kaggle.com/c/avazu-ctr-prediction/data
public class ClickThroughRateAnalytics {

	private static SimpleDateFormat sdf = new SimpleDateFormat("yyMMddHH");

	public static void main(String[] args) {

		final SparkConf sparkConf = new SparkConf().setAppName("Click Analysis").setMaster("local");

		try (JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf)) {

			SQLContext sqlContext = new SQLContext(javaSparkContext);
			DataFrame dataFrame = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true")
					.load("/splits/sub-suaa");

			// This will keep data in memory
			dataFrame.cache();

			// This will describe the column
			// dataFrame.describe("hour").show();

			System.out.println("Rows before removing missing data : " + dataFrame.count());

			// This will describe column details
			// dataFrame.describe("click", "hour", "site_domain").show();

			// This will calculate variance between columns +ve one increases
			// second increases and -ve means one increases other decreases
			// double cov = dataFrame.stat().cov("click", "hour");
			// System.out.println("cov : " + cov);

			// It provides quantitative measurements of the statistical
			// dependence between two random variables
			// double corr = dataFrame.stat().corr("click", "hour");
			// System.out.println("corr : " + corr);

			// Cross Tabulation provides a table of the frequency distribution
			// for a set of variables
			// dataFrame.stat().crosstab("site_id", "site_domain").show();

			// For frequent items
			// System.out.println("Frequest Items : " +
			// dataFrame.stat().freqItems(new String[] { "site_id",
			// "site_domain" }, 0.3).collectAsList());

			// TODO we can also set maximum occurring item to categorical
			// values.

			// This will replace null values with average for numeric columns
			dataFrame = modifiyDatFrame(dataFrame);

			// Removing rows which have some missing values
			dataFrame = dataFrame.na().replace(dataFrame.columns(), ImmutableMap.of("", "NA"));
			dataFrame.na().fill(0.0);
			dataFrame = dataFrame.na().drop();

			System.out.println("Rows after removing missing data : " + dataFrame.count());

			// TODO Binning and bucketing

			// normalizer will take the column created by the VectorAssembler,
			// normalize it and produce a new column
			// Normalizer normalizer = new
			// Normalizer().setInputCol("features_index").setOutputCol("features");

			dataFrame = dataFrame.drop("app_category_index").drop("app_domain_index").drop("hour_index").drop("C20_index")
					.drop("device_connection_type_index").drop("C1_index").drop("id").drop("device_ip_index").drop("banner_pos_index");
			DataFrame[] splits = dataFrame.randomSplit(new double[] { 0.7, 0.3 });
			DataFrame trainingData = splits[0];
			DataFrame testData = splits[1];

			StringIndexerModel labelIndexer = new StringIndexer().setInputCol("click").setOutputCol("indexedclick").fit(dataFrame);
			// Here we will be sending all columns which will participate in
			// prediction
			VectorAssembler vectorAssembler = new VectorAssembler().setInputCols(findPredictionColumns("click", dataFrame))
					.setOutputCol("features_index");

			GBTClassifier gbt = new GBTClassifier().setLabelCol("indexedclick").setFeaturesCol("features_index").setMaxIter(10).setMaxBins(69000);

			IndexToString labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel");
			Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] { labelIndexer, vectorAssembler, gbt, labelConverter });

			trainingData.show(1);
			PipelineModel model = pipeline.fit(trainingData);
			DataFrame predictions = model.transform(testData);
			predictions.select("predictedLabel", "label").show(5);
			MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel")
					.setPredictionCol("prediction").setMetricName("precision");
			double accuracy = evaluator.evaluate(predictions);
			System.out.println("Test Error = " + (1.0 - accuracy));

			GBTClassificationModel gbtModel = (GBTClassificationModel) (model.stages()[2]);

			System.out.println("Learned classification GBT model:\n" + gbtModel.toDebugString());

		}
	}

	private static String[] findPredictionColumns(String outputCol, DataFrame dataFrame) {
		String columns[] = dataFrame.columns();
		String inputColumns[] = new String[columns.length - 1];
		int count = 0;
		for (String column : dataFrame.columns()) {
			if (!column.equalsIgnoreCase(outputCol)) {
				inputColumns[count++] = column;
			}
		}
		return inputColumns;
	}

	/**
	 * This will replace empty values with mean.
	 * 
	 * @param columnName
	 * @param dataFrame
	 * @return
	 */
	private static DataFrame modifiyDatFrame(DataFrame dataFrame) {
		Set<String> numericColumns = new HashSet<String>();
		if (dataFrame.numericColumns() != null && dataFrame.numericColumns().length() > 0) {
			scala.collection.Iterator<Expression> iterator = ((Seq<Expression>) dataFrame.numericColumns()).toIterator();
			while (iterator.hasNext()) {
				Expression expression = iterator.next();
				Double avgAge = dataFrame.na().drop().groupBy(((AttributeReference) expression).name()).avg(((AttributeReference) expression).name())
						.first().getDouble(1);
				dataFrame = dataFrame.na().fill(avgAge, new String[] { ((AttributeReference) expression).name() });
				numericColumns.add(((AttributeReference) expression).name());

				DataType dataType = ((AttributeReference) expression).dataType();
				if (!"double".equalsIgnoreCase(dataType.simpleString())) {
					dataFrame = dataFrame.withColumn("temp", dataFrame.col(((AttributeReference) expression).name()).cast(DataTypes.DoubleType))
							.drop(((AttributeReference) expression).name()).withColumnRenamed("temp", ((AttributeReference) expression).name());
				}
			}
		}

		// Fit method of StringIndexer converts the column to StringType(if
		// it is not of StringType) and then counts the occurrence of each
		// word. It then sorts these words in descending order of their
		// frequency and assigns an index to each word. StringIndexer.fit()
		// method returns a StringIndexerModel which is a Transformer
		StringIndexer stringIndexer = new StringIndexer();
		String allCoumns[] = dataFrame.columns();
		for (String column : allCoumns) {
			if (!numericColumns.contains(column)) {
				dataFrame = stringIndexer.setInputCol(column).setOutputCol(column + "_index").fit(dataFrame).transform(dataFrame);
				dataFrame = dataFrame.drop(column);
			}
		}

		dataFrame.printSchema();
		return dataFrame;
	}

	@SuppressWarnings("unused")
	private static void copyFile(DataFrame dataFrame) {
		dataFrame
				.select("id", "click", "hour", "C1", "banner_pos", "site_id", "site_domain", "site_category", "app_id", "app_domain", "app_category",
						"device_id", "device_ip", "device_model", "device_type", "device_conn_type", "C14", "C15", "C16", "C17", "C18", "C19", "C20",
						"C21")
				.write().format("com.databricks.spark.csv").option("header", "true").option("codec", "org.apache.hadoop.io.compress.GzipCodec")
				.save("/splits/sub-splitaa-optmized");
	}

	@SuppressWarnings("unused")
	private static Integer parse(String sDate, int field) {
		try {
			if (sDate != null && !sDate.toString().equalsIgnoreCase("hour")) {
				Date date = sdf.parse(sDate.toString());
				Calendar cal = Calendar.getInstance();
				cal.setTime(date);
				return cal.get(field);
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return 0;
	}

}
