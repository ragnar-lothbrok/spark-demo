package com.spark.lograthmicregression;

import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;

import ml.dmlc.xgboost4j.scala.DMatrix;
import ml.dmlc.xgboost4j.scala.spark.XGBoostModel;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;

public class ClickThroughRateAnalytics_XGboost_SPARK {

	public static void main(String[] args) {
		try {
			final SparkConf sparkConf = new SparkConf().setAppName("Click Analysis Using Spark").setMaster("local");
			sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
			sparkConf.set("spark.task.cpus", "4");
			SparkContext javaSparkContext = new SparkContext(sparkConf);

			RDD<LabeledPoint> trainData = MLUtils.loadLibSVMFile(javaSparkContext, "/home/raghunandangupta/Downloads/train_1.data");
			HashMap<String, Object> params = new HashMap<String, Object>();
			params.put("eta", 0.18);
			params.put("n_estimators", 113);
			params.put("max_depth", 9);
			params.put("min_child_weight", 1);
			params.put("subsample", 0.8);
			params.put("colsample_bytree", 0.8);
			params.put("objective", "binary:logistic");
			params.put("nthread", 8);
			params.put("alpha", 0.001);
			params.put("scale_pos_weight", 1);
			params.put("silent", true);
			params.put("seed", 27);
			int numRound = 10;
			XGBoostModel xgBoostModel = ml.dmlc.xgboost4j.scala.spark.XGBoost.train(trainData,
					convert(params), 4, numRound, null, null, true);
			float[][] predictions = xgBoostModel.predict(new DMatrix("/home/raghunandangupta/Downloads/train_1.data"));
			System.out.println(predictions.length);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static <K, V> Map<K, V> convert(java.util.Map<K, V> m) {
	    return JavaConverters.mapAsScalaMapConverter(m).asScala().toMap(
	      scala.Predef$.MODULE$.<scala.Tuple2<K, V>>conforms()
	    );
	  }

}
