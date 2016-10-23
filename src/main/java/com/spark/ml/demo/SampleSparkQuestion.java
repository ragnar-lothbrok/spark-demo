package com.spark.ml.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;

import scala.Tuple2;

//https://datahack.analyticsvidhya.com/contest/knocktober-2016/
public class SampleSparkQuestion {

	public static void main(String[] args) {

		convertToCSV();
	}

	private static void method_1() {
		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("Libsvm Convertor");

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

		JavaRDD<String> rdd = javaSparkContext.textFile("/home/raghunandangupta/Downloads/soc_gen_data/abc.txt");

		rdd.map(new Function<String, Tuple2<String, String>>() {

			@Override
			public Tuple2<String, String> call(String v1) throws Exception {
				String split[] = v1.split(",");
				split[0] = split[0].trim();
				split[1] = split[1].trim();
				if (split[0].equalsIgnoreCase("time")) {
					return new Tuple2<String, String>(split[0], split[1].trim());
				} else {
					return new Tuple2<String, String>((Integer.parseInt(split[0]) + 3000) + "", split[1].trim());
				}
			}
		}).repartition(1).saveAsTextFile("/home/raghunandangupta/Downloads/soc_gen_data/abc/");
	}

	private static void convertToCSV() {
		SparkConf sparkConf = new SparkConf().setAppName("Java Decision Tree Classification");
		sparkConf.setMaster("local");
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(javaSparkContext);

		final DataFrame firstHealthCampDF = sqlContext.read().format("com.databricks.spark.csv").option("header", "true")
				.load("/home/raghunandangupta/Downloads/Train_2/Train/First_Health_Camp_Attended.csv");

		final DataFrame secondHealthCampDF = sqlContext.read().format("com.databricks.spark.csv").option("header", "true")
				.load("/home/raghunandangupta/Downloads/Train_2/Train/Second_Health_Camp_Attended.csv");

		DataFrame aggregateDF = firstHealthCampDF.selectExpr(new String[] { "Patient_ID", "Health_Camp_ID", "Health_Score" })
				.unionAll(secondHealthCampDF);

		sqlContext.registerDataFrameAsTable(aggregateDF, "aggregateDF");

		final DataFrame campDF = sqlContext.read().format("com.databricks.spark.csv").option("header", "true")
				.load("/home/raghunandangupta/Downloads/Train_2/Train/Health_Camp_Detail.csv");

		sqlContext.registerDataFrameAsTable(campDF, "campDF");

		final DataFrame patientDF = sqlContext.read().format("com.databricks.spark.csv").option("header", "true")
				.load("/home/raghunandangupta/Downloads/Train_2/Train/Patient_Profile.csv");

		sqlContext.registerDataFrameAsTable(patientDF, "patientDF");

		final DataFrame trainDF = sqlContext.read().format("com.databricks.spark.csv").option("header", "true")
				.load("/home/raghunandangupta/Downloads/Train_2/Train/Train.csv");

		sqlContext.registerDataFrameAsTable(trainDF, "trainDF");
		
		
		
		
		
		
		
		sqlContext.udf().register("convertToZero", (String abc) -> ((abc != null && abc.trim().length() > 0 && (!"None".equalsIgnoreCase(abc.trim()) || !"null".equalsIgnoreCase(abc.trim()))) ? abc : "0"), DataTypes.StringType);
		sqlContext.udf().register("convertToText", (String abc) -> ((abc != null && abc.trim().length() > 0 && (!"None".equalsIgnoreCase(abc.trim()) || !"null".equalsIgnoreCase(abc.trim()))) ? abc : "Z"), DataTypes.StringType);
		
		

		final DataFrame intermediateDF = sqlContext.sql(
				"select ad.Patient_ID,ad.Health_Camp_ID,ad.Health_Score,cd.Camp_Start_Date,cd.Camp_End_Date,cd.Category1,cd.Category2,cd.Category3,pd.Income,pd.Education_Score,pd.Age,pd.First_Interaction,pd.City_Type,pd.Employer_Category from aggregateDF ad inner join campDF cd on ad.Health_Camp_ID=cd.Health_Camp_ID inner join patientDF pd on pd.Patient_ID = ad.Patient_ID");
		sqlContext.registerDataFrameAsTable(intermediateDF, "intermediateDF");

		final DataFrame finalDF = sqlContext.sql(
				"select fd.*,id.Health_Score,id.Camp_Start_Date,id.Camp_End_Date,id.Category1,id.Category2,id.Category3,id.Income,id.Education_Score,id.Age,id.First_Interaction,id.City_Type,id.Employer_Category from trainDF fd left join intermediateDF id on id.Health_Camp_ID=fd.Health_Camp_ID and id.Patient_ID = fd.Patient_ID");
		finalDF.printSchema();

		final DataFrame testDF = sqlContext.read().format("com.databricks.spark.csv").option("header", "true")
				.load("/home/raghunandangupta/Downloads/Train_2/Train/Test_D7W1juQ.csv");
		sqlContext.registerDataFrameAsTable(testDF, "testDF");
		DataFrame finalTestDF = sqlContext.sql(
				"select td.*,cd.Camp_Start_Date,cd.Camp_End_Date,cd.Category1,cd.Category2,cd.Category3,pd.Income,pd.Education_Score,pd.Age,pd.First_Interaction,pd.City_Type,pd.Employer_Category from testDF td inner join campDF cd on td.Health_Camp_ID=cd.Health_Camp_ID inner join patientDF pd on pd.Patient_ID = td.Patient_ID");
		finalTestDF.printSchema();

		System.out.println("$$$$$$$$" + finalDF.count() + " " + finalTestDF.count());
		
		finalDF.selectExpr("Patient_ID","Health_Camp_ID","Registration_Date","Var1","Var2","Var3","Var4","Var5","convertToZero(Health_Score) as Health_Score","Camp_Start_Date","Camp_End_Date","Category1","Category2","Category3","convertToZero(Income) as Income","Education_Score","Age","First_Interaction","convertToText(City_Type)","convertToText(Employer_Category)").repartition(1).write().format("com.databricks.spark.csv").option("header", "true").save("/home/raghunandangupta/Downloads/Train_2/Train/Train_1.csv");
		finalTestDF.selectExpr("Patient_ID","Health_Camp_ID","Registration_Date","Var1","Var2","Var3","Var4","Var5","Camp_Start_Date","Camp_End_Date","Category1","Category2","Category3","Income","Education_Score","Age","First_Interaction","convertToText(City_Type)","convertToText(Employer_Category)").repartition(1).write().format("com.databricks.spark.csv").option("header", "true").save("/home/raghunandangupta/Downloads/Train_2/Train/Test_1.csv");

	}
}
