package com.spark.kmeans.brainwaves;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

//http://www.dataonfocus.com/k-means-clustering-example-and-algorithm/
public class KMeans {

	// Number of Clusters. This metric should be related to the number of points
	private int NUM_CLUSTERS = 20;

	// Min and Max X and Y
	private static double MIN_YCOORDINATE = Double.MAX_VALUE;
	private static double MAX_YCOORDINATE = Double.MIN_VALUE;

	private static double MIN_XCOORDINATE = Double.MAX_VALUE;
	private static double MAX_XCOORDINATE = Double.MIN_VALUE;

	private static List<Point> points;
	private List<Cluster> clusters;

	public KMeans() {
		this.clusters = new ArrayList<Cluster>();
	}

	public static void main(String[] args) throws Exception {

		@SuppressWarnings("resource")
		BufferedReader br = new BufferedReader(new FileReader("/home/raghunandangupta/Downloads/soc_gen_data/train.csv"));
		transform_2(br);
		KMeans kmeans = new KMeans();
		kmeans.init();
		kmeans.calculate();

	}

	private static void transform_2(BufferedReader br) throws IOException {
		String line = br.readLine();
		points = new ArrayList<Point>();
		double med = 0;
		while ((line = br.readLine()) != null) {
			String split[] = line.split(",");
			for (int i = 1; i <= 100; i++) {
				MIN_YCOORDINATE = 0;

				if (MAX_YCOORDINATE < Math.abs(med - Double.parseDouble(split[i]))) {
					MAX_YCOORDINATE = Math.abs(med - Double.parseDouble(split[i]));
				}

				Double time = Double.parseDouble(split[0]);
				if (MIN_XCOORDINATE > time) {
					MIN_XCOORDINATE = time;
				}

				if (MAX_XCOORDINATE < time) {
					MAX_XCOORDINATE = time;
				}

				if (i == 0) {
					med = Double.parseDouble(split[i]);
					points.add(new Point(Double.parseDouble(split[0]), 0, "X" + i));
				} else {
					points.add(new Point(Double.parseDouble(split[0]), Math.abs(med - Double.parseDouble(split[i])), "X" + i));
					med = Double.parseDouble(split[i]);
				}
			}

		}

	}

	private static void transform_1(BufferedReader br) throws IOException {
		String line = br.readLine();
		points = new ArrayList<Point>();
		while ((line = br.readLine()) != null) {
			String split[] = line.split(",");
			for (int i = 1; i <= 100; i++) {
				Double temp = Double.parseDouble(split[i]);
				if (MIN_YCOORDINATE > temp) {
					MIN_YCOORDINATE = temp;
				}

				if (MAX_YCOORDINATE < temp) {
					MAX_YCOORDINATE = temp;
				}

				Double time = Double.parseDouble(split[0]);
				if (MIN_XCOORDINATE > time) {
					MIN_XCOORDINATE = time;
				}

				if (MAX_XCOORDINATE < time) {
					MAX_XCOORDINATE = time;
				}

				if (isSqrt) {
					points.add(new Point(Double.parseDouble(split[0]), Math.sqrt(Double.parseDouble(split[i])), "X" + i));
				} else {
					points.add(new Point(Double.parseDouble(split[0]), Double.parseDouble(split[i]), "X" + i));
				}

			}
		}
	}

	private static boolean isSqrt = false;

	// Initializes the process
	public void init() {

		if (isSqrt) {
			MAX_YCOORDINATE = Math.sqrt(MAX_YCOORDINATE);
			MIN_YCOORDINATE = Math.sqrt(MIN_YCOORDINATE);
		}
		for (int i = 0; i < NUM_CLUSTERS; i++) {
			Cluster cluster = new Cluster(i);
			Random r = new Random();
			double x = MIN_XCOORDINATE + (MAX_XCOORDINATE - MIN_XCOORDINATE) * r.nextDouble();
			double y = MIN_YCOORDINATE + (MAX_YCOORDINATE - MIN_YCOORDINATE) * r.nextDouble();
			Point centroid = new Point(x, y, null);

			cluster.setCentroid(centroid);
			clusters.add(cluster);
		}
	}

	private static Map<String, Map<String, Integer>> map = new LinkedHashMap<String, Map<String, Integer>>();

	private void plotClusters() {
		map.clear();
		for (int i = 0; i < NUM_CLUSTERS; i++) {
			Cluster c = clusters.get(i);
			for (Point point : c.getPoints()) {
				if (map.get(point.getName()) == null) {
					map.put(point.getName(), new LinkedHashMap<String, Integer>());
				}
				if (map.get(point.getName()).get(c.getId() + "") != null) {
					map.get(point.getName()).put(c.getId() + "", map.get(point.getName()).get(c.getId() + "") + 1);
				} else {
					map.get(point.getName()).put(c.getId() + "", 1);
				}
			}
		}

		Map<String, String> finalCluster = new LinkedHashMap<String, String>();

		List<String> finalClusters = new ArrayList<String>();

		for (Entry<String, Map<String, Integer>> entry : map.entrySet()) {
			int max = Integer.MIN_VALUE;
			String cluster = "";
			for (Entry<String, Integer> subEntry : entry.getValue().entrySet()) {
				if (max < subEntry.getValue()) {
					max = subEntry.getValue();
					cluster = subEntry.getKey();
				}
			}
			finalCluster.put(entry.getKey(), cluster);
			if (!finalClusters.contains(cluster)) {
				finalClusters.add(cluster);
			}
		}

		System.out.println("######" + finalClusters);

		StringBuilder sb = new StringBuilder();
		sb.append("Asset,Cluster\n");

		for (int i = 1; i <= 100; i++) {
			sb.append("X" + i + "," + (finalClusters.indexOf(finalCluster.get("X" + i))) + "\n");
		}
		Map<String, Integer> clusterCountMap = new LinkedHashMap<String, Integer>();
		for (Entry<String, String> entry : finalCluster.entrySet()) {
			if (clusterCountMap.get(entry.getValue()) != null) {
				clusterCountMap.put(entry.getValue(), clusterCountMap.get(entry.getValue()) + 1);
			} else {
				clusterCountMap.put(entry.getValue(), 1);
			}
		}
		System.out.println(sb.toString());
	}

	// The process to calculate the K Means, with iterating method.
	public void calculate() {
		boolean finish = false;
		int iteration = 0;

		// Add in new data, one at a time, recalculating centroids with each new
		// one.
		while (!finish) {
			// Clear cluster state
			clearClusters();

			List<Point> lastCentroids = getCentroids();

			// Assign points to the closer cluster
			assignCluster();

			// Calculate new centroids.
			calculateCentroids();

			iteration++;

			List<Point> currentCentroids = getCentroids();

			// Calculates total distance between new and old Centroids
			double distance = 0;
			for (int i = 0; i < lastCentroids.size(); i++) {
				distance += Point.distance(lastCentroids.get(i), currentCentroids.get(i));
			}
			System.out.println("#################");
			System.out.println("Iteration: " + iteration);
			System.out.println("Centroid distances: " + distance);
			plotClusters();

			if (distance == 0) {
				finish = true;
			}
		}
	}

	private void clearClusters() {
		for (Cluster cluster : clusters) {
			cluster.clear();
		}
	}

	private List<Point> getCentroids() {
		List<Point> centroids = new ArrayList<Point>(NUM_CLUSTERS);
		for (Cluster cluster : clusters) {
			Point aux = cluster.getCentroid();
			Point point = new Point(aux.getX(), aux.getY(), null);
			centroids.add(point);
		}
		return centroids;
	}

	private void assignCluster() {
		double max = Double.MAX_VALUE;
		double min = max;
		int cluster = 0;
		double distance = 0.0;

		for (Point point : points) {
			min = max;
			for (int i = 0; i < NUM_CLUSTERS; i++) {
				Cluster c = clusters.get(i);
				distance = Point.distance(point, c.getCentroid());
				if (distance < min) {
					min = distance;
					cluster = i;
				}
			}
			point.setCluster_number(cluster);
			clusters.get(cluster).addPoint(point);
		}
	}

	private void calculateCentroids() {
		for (Cluster cluster : clusters) {
			double sumX = 0;
			double sumY = 0;
			List<Point> list = cluster.getPoints();
			int n_points = list.size();

			for (Point point : list) {
				sumX += point.getX();
				sumY += point.getY();
			}

			Point centroid = cluster.getCentroid();
			if (n_points > 0) {
				double newX = sumX / n_points;
				double newY = sumY / n_points;
				centroid.setX(newX);
				centroid.setY(newY);
			}
		}
	}

}
