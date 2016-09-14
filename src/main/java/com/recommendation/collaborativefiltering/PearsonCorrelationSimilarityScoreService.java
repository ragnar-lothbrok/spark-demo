package com.recommendation.collaborativefiltering;

import java.util.ArrayList;
import java.util.List;

import com.recommendation.collaborativefiltering.UserData.MovieRating;

/**
 * When people have rated different for Good,Average and Bad
 * 
 * @author raghunandangupta
 *
 */
public class PearsonCorrelationSimilarityScoreService implements SimilarityScoreService {

	@Override
	public Double similarityScore(UserData u1, UserData u2) {
		List<MovieRating> commonMovies = new ArrayList<MovieRating>();

		for (MovieRating movieRating : u1.getMovieRatings()) {
			if (u2.getMovieRatings().contains(movieRating)) {
				commonMovies.add(movieRating);
			}
		}
		return calculatePearsonCorrelationDistance(u1, u2, commonMovies);
	}

	private Double calculatePearsonCorrelationDistance(UserData u1, UserData u2, List<MovieRating> commonMovies) {
		double person1_preferences_sum = 0.0;
		double person2_preferences_sum = 0.0;
		double person1_square_preferences_sum = 0.0;
		double person2_square_preferences_sum = 0.0;
		double product_sum_of_both_users = 0.0;
		for (MovieRating movieRating : commonMovies) {

			person1_preferences_sum += u1.getMovieRatings().get(u1.getMovieRatings().indexOf(movieRating)).getMovieRating();
			person2_preferences_sum += u2.getMovieRatings().get(u2.getMovieRatings().indexOf(movieRating)).getMovieRating();

			person1_square_preferences_sum += Math.pow(u1.getMovieRatings().get(u1.getMovieRatings().indexOf(movieRating)).getMovieRating(), 2);
			person2_square_preferences_sum += Math.pow(u2.getMovieRatings().get(u2.getMovieRatings().indexOf(movieRating)).getMovieRating(), 2);

			product_sum_of_both_users += Math.abs(u1.getMovieRatings().get(u1.getMovieRatings().indexOf(movieRating)).getMovieRating()
					* u2.getMovieRatings().get(u2.getMovieRatings().indexOf(movieRating)).getMovieRating());
		}
		return (product_sum_of_both_users - (person1_preferences_sum * person2_preferences_sum / commonMovies.size()))
				/ Math.sqrt((person2_square_preferences_sum - (Math.pow(person2_preferences_sum, 2) / commonMovies.size()))
						* (person1_square_preferences_sum - (Math.pow(person1_preferences_sum, 2) / commonMovies.size())));
	}

}
