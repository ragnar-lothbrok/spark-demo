package com.spark.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class BasketMarketAnalysis {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("Basket Analysis");

		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

		SQLContext sqlContext = new SQLContext(sparkContext);

		// Stack Overflow
		List<String> stringAsList = new ArrayList<String>();
		stringAsList.add("buzz");

		JavaRDD<Row> rowRDD = sparkContext.parallelize(stringAsList).map((String row) -> {
			return RowFactory.create(row);
		});

		StructType schema = DataTypes.createStructType(new StructField[] { DataTypes.createStructField("fizz", DataTypes.StringType, false) });

		DataFrame df = sqlContext.createDataFrame(rowRDD, schema).toDF();
		df.show();

		DataFrame crimeDataFrame = sqlContext.read().format("com.databricks.spark.csv").option("inferSchema", "true").option("header", "false")
				.load("/tmp/file.txt");

		sqlContext.registerDataFrameAsTable(crimeDataFrame, "basket");

		System.out.println(crimeDataFrame.count());

		System.out.println(crimeDataFrame.na().drop().count());

		sqlContext.udf().register("convertToNull", (String abc) -> (abc.trim().length() > 0 ? abc : null), DataTypes.StringType);

		System.out.println(crimeDataFrame.selectExpr("C0", "convertToNull(C1)", "C2", "C3").na().drop().count());

		DataFrame topDF = sqlContext.sql("select distinct(C3) from basket order by C3 DESC limit 3");

		sqlContext.registerDataFrameAsTable(topDF, "topDF");

		DataFrame joinedDataFrame = sqlContext.sql("select b.C1,b.C3 from basket b join topDF td where td.C3 = b.C3");

		List<List<String>> result = joinedDataFrame.javaRDD().mapToPair(new PairFunction<Row, Integer, List<String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, List<String>> call(Row t) throws Exception {
				List<String> list = new ArrayList<String>();
				list.add(t.getString(0));
				return new Tuple2<Integer, List<String>>(((Double) t.getDouble(1)).intValue(), list);
			}
		}).reduceByKey(new Function2<List<String>, List<String>, List<String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public List<String> call(List<String> v1, List<String> v2) throws Exception {
				v1.addAll(v2);
				return v1;
			}
		}).sortByKey(false).values().collect();

		System.out.println(result);

	}
}
