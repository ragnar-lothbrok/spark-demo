package com.recommendation.collaborativefiltering;

import java.util.List;

public interface RecommendationService {

	List<String> getRecommendedMovies(List<UserData> allUsers,UserData u1);
}
