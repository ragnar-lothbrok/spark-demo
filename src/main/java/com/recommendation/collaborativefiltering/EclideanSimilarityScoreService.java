package com.recommendation.collaborativefiltering;

import java.util.ArrayList;
import java.util.List;

import com.recommendation.collaborativefiltering.UserData.MovieRating;

public class EclideanSimilarityScoreService implements SimilarityScoreService {

	@Override
	public Double similarityScore(UserData u1, UserData u2) {

		List<MovieRating> commonMovies = new ArrayList<MovieRating>();

		for (MovieRating movieRating : u1.getMovieRatings()) {
			if (u2.getMovieRatings().contains(movieRating)) {
				commonMovies.add(movieRating);
			}
		}
		return calculateEclideanDistance(u1, u2, commonMovies);
	}

	private Double calculateEclideanDistance(UserData u1, UserData u2, List<MovieRating> commonMovies) {
		double sum = 0.0;
		for (MovieRating movieRating : commonMovies) {
			sum += Math.abs(Math.pow((u1.getMovieRatings().get(u1.getMovieRatings().indexOf(movieRating)).getMovieRating()
					- u2.getMovieRatings().get(u2.getMovieRatings().indexOf(movieRating)).getMovieRating()), 2));
		}
		return 1.0 / (1 + Math.sqrt(sum));
	}

}
