package com.flink;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

//http://data-flair.training/blogs/apache-flink-use-case-crime-data-analysis/
public class CrimeDataAnalysis {

	public static void main(String[] args) {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<String, String>> rawdata = env.readCsvFile("/SacramentocrimeJanuary2006.csv")
				.includeFields("0000011").ignoreFirstLine().types(String.class, String.class);
	}

	public static class CrimeCounter implements GroupReduceFunction<Tuple2<String, String>, Tuple3<String, String, Integer>> {

		@Override
		public void reduce(Iterable<Tuple2<String, String>> records, Collector<Tuple3<String, String, Integer>> out) throws Exception {

			String crimerecord = null;
			String ucr_code = null;
			int cnt = 0;

			// count number of tuples
			for (Tuple2<String, String> m : records) {

				crimerecord = m.f0;
				ucr_code = m.f1;

				// increase count
				cnt++;
			}

			// emit crimerecord, ucr_code, and count
			out.collect(new Tuple3<>(crimerecord, ucr_code, cnt));
		}
	}
}
