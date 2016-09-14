package com.recommendation.collaborativefiltering;

public interface SimilarityScoreService {

	Double similarityScore(UserData u1, UserData u2);
}
