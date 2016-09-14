package com.recommendation.collaborativefiltering;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class UserData implements Serializable {

	private static final long serialVersionUID = 1L;

	private String userName;

	private List<MovieRating> movieRatings = new ArrayList<MovieRating>();

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public List<MovieRating> getMovieRatings() {
		return movieRatings;
	}

	public void setMovieRatings(List<MovieRating> movieRatings) {
		this.movieRatings = movieRatings;
	}

	public UserData() {

	}

	public UserData(String userName) {
		super();
		this.userName = userName;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((userName == null) ? 0 : userName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		UserData other = (UserData) obj;
		if (userName == null) {
			if (other.userName != null)
				return false;
		} else if (!userName.equals(other.userName))
			return false;
		return true;
	}

	static class MovieRating {
		private String movieName;
		private Double movieRating;

		public String getMovieName() {
			return movieName;
		}

		public void setMovieName(String movieName) {
			this.movieName = movieName;
		}

		public Double getMovieRating() {
			return movieRating;
		}

		public void setMovieRating(Double movieRating) {
			this.movieRating = movieRating;
		}

		public MovieRating() {
		}

		public MovieRating(String movieName, Double movieRating) {
			super();
			this.movieName = movieName;
			this.movieRating = movieRating;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((movieName == null) ? 0 : movieName.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			MovieRating other = (MovieRating) obj;
			if (movieName == null) {
				if (other.movieName != null)
					return false;
			} else if (!movieName.equals(other.movieName))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "MovieRating [movieName=" + movieName + "]";
		}

	}
}
