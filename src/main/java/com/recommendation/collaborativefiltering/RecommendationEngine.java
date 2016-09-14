package com.recommendation.collaborativefiltering;

import java.util.List;

//http://dataaspirant.com/2015/05/25/collaborative-filtering-recommendation-engine-implementation-in-python/?subscribe=success#blog_subscription-18
public class RecommendationEngine {

	public static void main(String[] args) {

		List<UserData> userDataList = JsonParserUtility
				.parseJson("/home/raghunandangupta/gitPro/spark-demo/src/main/java/com/recommendation/collaborativefiltering/data.json");

//		System.out.println(new EclideanSimilarityScoreService().similarityScore(userDataList.get(userDataList.indexOf(new UserData("Lisa Rose"))),
//				userDataList.get(userDataList.indexOf(new UserData("Jack Matthews")))));
//		
//		System.out.println(new PearsonCorrelationSimilarityScoreService().similarityScore(userDataList.get(userDataList.indexOf(new UserData("Lisa Rose"))),
//				userDataList.get(userDataList.indexOf(new UserData("Jack Matthews")))));
		
		System.out.println(new PearsonCorrelationSimilarityScoreService().similarityScore(userDataList.get(userDataList.indexOf(new UserData("Michael Phillips"))),
				userDataList.get(userDataList.indexOf(new UserData("Toby")))));
		
		
		System.out.println(new MovieRecommendationService().getRecommendedMovies(userDataList, userDataList.get(userDataList.indexOf(new UserData("Toby")))));
		
	}
}
