package com.spark.lograthmicregression;

import java.util.HashMap;

import ml.dmlc.xgboost4j.java.Booster;
import ml.dmlc.xgboost4j.java.DMatrix;
import ml.dmlc.xgboost4j.java.XGBoost;
import ml.dmlc.xgboost4j.java.XGBoostError;

public class ClickThroughRateAnalytics_XGboost {

	public static void main(String[] args) {
		try {
			DMatrix trainMat = new DMatrix("/home/raghunandangupta/Downloads/abcdaa");
			DMatrix testMat = new DMatrix("/home/raghunandangupta/Downloads/train_1.data");
			
			// specify parameters
			HashMap<String, Object> params = new HashMap<String, Object>();

			params.put("eta", 0.18);
			params.put("n_estimators", 113);
			params.put("max_depth", 9);
			params.put("min_child_weight", 1);
//			params.put("gamma", 0);
			params.put("subsample", 0.8);
			params.put("colsample_bytree", 0.8);
			params.put("objective", "binary:logistic");
			params.put("nthread", 8);
			params.put("alpha", 0.001);
			params.put("scale_pos_weight", 1);
			params.put("silent", true);
			params.put("seed", 27);

			// do 5-fold cross validation
			int round = 500;
			int nfold = 8;
			// set additional eval_metrics
			String[] metrics = new String[] { "auc" };

			String[] evalHist = XGBoost.crossValidation(trainMat, params, round, nfold, metrics, null, null);

			params.put("eval_metric", "logloss");
			params.put("silent", false);
			
			// specify watchList
			HashMap<String, DMatrix> watches = new HashMap<String, DMatrix>();
			watches.put("train", trainMat);
			watches.put("test", testMat);

			// train a booster
			Booster booster = XGBoost.train(trainMat, params, evalHist.length, watches, null, null);

			// predict use 1 tree
			float[][] predicts1 = booster.predict(trainMat);
			// by default all trees are used to do predict
			float[][] predicts2 = booster.predict(testMat);

			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			// use a simple evaluation class to check error result
			CustomEval eval = new CustomEval();
			System.out.println("error of predicts1: " + eval.eval(predicts1, trainMat));
			System.out.println("error of predicts2: " + eval.eval(predicts2, testMat));

		} catch (XGBoostError e) {
			e.printStackTrace();
		}
	}

}
