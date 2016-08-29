package com.spark.lograthmicregression;

import ml.dmlc.xgboost4j.java.DMatrix;
import ml.dmlc.xgboost4j.java.IEvaluation;
import ml.dmlc.xgboost4j.java.XGBoostError;

public class CustomEval implements IEvaluation {

	private static final long serialVersionUID = 1L;
	String evalMetric = "custom_error";

	@Override
	public String getMetric() {
		return evalMetric;
	}

	@Override
	public float eval(float[][] predicts, DMatrix dmat) {
		

		int[][] confusionMatrix = new int[3][3];
		confusionMatrix[0][1] = 0;
		confusionMatrix[0][2] = 1;
		confusionMatrix[1][0] = 0;
		confusionMatrix[2][0] = 1;

		float error = 0f;
		float[] labels;
		try {
			labels = dmat.getLabel();
		} catch (XGBoostError ex) {
			return -1f;
		}
		int nrow = predicts.length;
		for (int i = 0; i < nrow; i++) {
			System.out.println(predicts[i][0]);
			if (labels[i] == 0f) {
				if (predicts[i][0] > 0.5) {
					confusionMatrix[1][2]++;
					error++;
				}else{
					confusionMatrix[1][1]++;
				}
			} else if (labels[i] == 1f) {
				if (predicts[i][0] <= 0.5) {
					confusionMatrix[2][1]++;
					error++;
				}else{
					confusionMatrix[2][2]++;
				}
			}
		}

		System.out.println(confusionMatrix[2][1] +confusionMatrix[1][2] );
		for (int i = 0; i < confusionMatrix.length; i++) {
			System.out.println(confusionMatrix[i][0] + "\t" + confusionMatrix[i][1] + "\t" + confusionMatrix[i][2]);
		}
		return error / labels.length;
	}
}
