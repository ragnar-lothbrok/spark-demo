package com.recommendation.collaborativefiltering;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.recommendation.collaborativefiltering.UserData.MovieRating;

public class JsonParserUtility {

	public static List<UserData> parseJson(String filePath) {

		List<UserData> userDataList = new ArrayList<UserData>();
		BufferedReader bufferedReader = null;
		try {
			bufferedReader = new BufferedReader(new FileReader(filePath));
			StringBuilder data = new StringBuilder();
			String line = bufferedReader.readLine();
			while (line != null) {
				data.append(line);
				line = bufferedReader.readLine();
			}

			TypeReference<HashMap<String, Map<String, Double>>> typeRef = new TypeReference<HashMap<String, Map<String, Double>>>() {
			};

			ObjectMapper mapper = new ObjectMapper();
			HashMap<String, Map<String, Double>> object = mapper.readValue(data.toString(), typeRef);

			Iterator<Entry<String, Map<String, Double>>> parentIterator = object.entrySet().iterator();
			while (parentIterator.hasNext()) {
				Entry<String, Map<String, Double>> parentEntry = parentIterator.next();
				UserData userData = new UserData();
				userData.setUserName(parentEntry.getKey());

				Iterator<Entry<String, Double>> childIterator = parentEntry.getValue().entrySet().iterator();

				while (childIterator.hasNext()) {
					Entry<String, Double> childEntry = childIterator.next();
					userData.getMovieRatings().add(new MovieRating(childEntry.getKey().trim().toLowerCase(), childEntry.getValue()));
				}

				userDataList.add(userData);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (bufferedReader != null) {
				try {
					bufferedReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return userDataList;
	}

}
