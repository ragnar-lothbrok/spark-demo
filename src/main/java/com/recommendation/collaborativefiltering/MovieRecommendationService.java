package com.recommendation.collaborativefiltering;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.recommendation.collaborativefiltering.UserData.MovieRating;

public class MovieRecommendationService implements RecommendationService {

	@Override
	public List<String> getRecommendedMovies(List<UserData> allUsers, UserData u1) {
		Map<MovieRating, Double> totalMap = new HashMap<MovieRating, Double>();
		Map<MovieRating, Double> simSums = new HashMap<MovieRating, Double>();

		for (UserData userData : allUsers) {
			if (!userData.equals(u1)) {
				Double coefficient = new PearsonCorrelationSimilarityScoreService().similarityScore(userData, u1);
				if (coefficient > 0) {
					for (MovieRating movieRating : userData.getMovieRatings()) {
						if (!u1.getMovieRatings().contains(movieRating)) {
							if (totalMap.get(movieRating) == null) {
								totalMap.put(movieRating, (movieRating.getMovieRating() * coefficient));
								simSums.put(movieRating, coefficient);
							} else {
								totalMap.put(movieRating, (movieRating.getMovieRating() * coefficient) + totalMap.get(movieRating));
								simSums.put(movieRating, coefficient + simSums.get(movieRating));
							}
						}
					}
				}
			}
		}

		for (Entry<MovieRating, Double> entry : totalMap.entrySet()) {
			entry.setValue(entry.getValue() / simSums.get(entry.getKey()));
		}

		System.out.println(totalMap);
		return null;
	}

}
