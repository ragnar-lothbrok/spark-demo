package com.spark.lograthmicregression;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.GBTClassificationModel;
import org.apache.spark.ml.classification.GBTClassifier;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import com.google.common.collect.ImmutableMap;

import ml.dmlc.xgboost4j.java.XGBoostError;
import scala.Tuple2;
import scala.collection.mutable.Seq;

//https://github.com/yu-iskw/click-through-rate-prediction
///https://www.kaggle.com/c/avazu-ctr-prediction/data
public class ClickThroughRateAnalytics {

	private static SimpleDateFormat sdf = new SimpleDateFormat("yyMMddHH");

	public static void main(String[] args) throws XGBoostError {
		
		final SparkConf sparkConf = new SparkConf().setAppName("Click Analysis").setMaster("local");

		try (JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf)) {

			SQLContext sqlContext = new SQLContext(javaSparkContext);
			DataFrame dataFrame = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "true")
					.load("/home/raghunandangupta/Downloads/splits/sub-testtaa");

			// This will keep data in memory
			dataFrame.cache();

			// This will describe the column
			dataFrame.describe("hour").show();

			System.out.println("Rows before removing missing data : " + dataFrame.count());

			// This will describe column details
			dataFrame.describe("click", "hour", "site_domain").show();

			// This will calculate variance between columns +ve one increases
			// second increases and -ve means one increases other decreases
			double cov = dataFrame.stat().cov("click", "hour");
			System.out.println("cov : " + cov);

			// It provides quantitative measurements of the statistical
			// dependence between two random variables
			double corr = dataFrame.stat().corr("click", "hour");
			System.out.println("corr : " + corr);

			// Cross Tabulation provides a table of the frequency distribution
			// for a set of variables
			dataFrame.stat().crosstab("site_id", "site_domain").show();

			// For frequent items
			// System.out.println("Frequest Items : " +
			dataFrame.stat().freqItems(new String[] { "site_id", "site_domain" }, 0.3).collectAsList();

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

			StringIndexerModel stringIndexerModel = new StringIndexer().setInputCol("click").setOutputCol("indexedclick").fit(dataFrame);
			dataFrame = stringIndexerModel.transform(dataFrame);
			dataFrame.printSchema();

			dataFrame = dataFrame.drop("id").drop("click").drop("device_ip_index");

			// Get predictor variable names
			String[] predictors = dataFrame.columns();
			predictors = (String[]) ArrayUtils.removeElement(predictors, "indexedclick");

			VectorAssembler vectorAssembler = new VectorAssembler().setInputCols(predictors).setOutputCol("features_index");
			dataFrame = vectorAssembler.transform(dataFrame);

			dataFrame = dataFrame.select("indexedclick", "features_index").cache();
			DataFrame[] splits = dataFrame.randomSplit(new double[] { 0.7, 0.3 });
			DataFrame trainingData = splits[0];
			DataFrame testData = splits[1];
			GBTClassifier gbt = new GBTClassifier().setLabelCol("indexedclick").setFeaturesCol("features_index").setMaxIter(20).setMaxBins(26900)
					.setMaxDepth(20).setMinInfoGain(0.0001).setStepSize(0.00001).setSeed(200).setLossType("logistic").setSubsamplingRate(0.2);

			IndexToString labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("rawPrediction")
					.setLabels(stringIndexerModel.labels());

			Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] { gbt, labelConverter });

			// Cross validator
			BinaryClassificationEvaluator binaryClassificationEvaluator = new BinaryClassificationEvaluator();
			ParamMap[] paramMaps = new ParamGridBuilder().build();
			CrossValidator crossValidator = new CrossValidator().setEvaluator(binaryClassificationEvaluator).setNumFolds(3)
					.setEstimatorParamMaps(paramMaps).setEstimator(pipeline);
			CrossValidatorModel crossValidatorModel = crossValidator.fit(trainingData);

			PipelineModel pipelineModel = pipeline.fit(trainingData);

			predictions(pipelineModel, testData);
			predictions(crossValidatorModel, testData);

			GBTClassificationModel gbtModel = (GBTClassificationModel) (pipelineModel.stages()[0]);
			// System.out.println("Learned classification GBT model:\n" +
			// gbtModel.toDebugString());

		}
	}

	private static void predictions(Model model, DataFrame testData) {
		DataFrame predictions = model.transform(testData);
		predictions = predictions.select("rawPrediction", "indexedclick");
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
		System.out.println("Total rows : " + testData.count());

	}

	/**
	 * This will replace empty values with mean.
	 * 
	 * @param columnName
	 * @param dataFrame
	 * @return
	 */
	private static DataFrame modifiyDatFrame(DataFrame dataFrame) {
		System.out.println(dataFrame.numericColumns());
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
		OneHotEncoder oneHotEncoder = new OneHotEncoder();
		String allCoumns[] = dataFrame.columns();
		for (String column : allCoumns) {
			if (!numericColumns.contains(column)) {
				dataFrame = stringIndexer.setInputCol(column).setOutputCol(column + "_index").setHandleInvalid("skip").fit(dataFrame)
						.transform(dataFrame);
				dataFrame = dataFrame.drop(column);
				dataFrame = oneHotEncoder.setInputCol(column + "_index").setOutputCol(column).transform(dataFrame);
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
