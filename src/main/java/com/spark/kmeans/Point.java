package com.spark.kmeans;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Point {

	private double x = 0;
	private double y = 0;
	private int cluster_number = 0;

	public double getX() {
		return x;
	}

	public void setX(double x) {
		this.x = x;
	}

	public double getY() {
		return y;
	}

	public void setY(double y) {
		this.y = y;
	}

	public int getCluster_number() {
		return cluster_number;
	}

	public void setCluster_number(int cluster_number) {
		this.cluster_number = cluster_number;
	}

	public Point() {

	}

	public Point(double x, double y) {
		super();
		this.x = x;
		this.y = y;
	}

	// Calculates the distance between two points.
	static double distance(Point p, Point centroid) {
		return Math.sqrt(Math.pow((centroid.getY() - p.getY()), 2) + Math.pow((centroid.getX() - p.getX()), 2));
	}

	// Creates random point
	protected static Point createRandomPoint(int min, int max) {
		Random r = new Random();
		double x = min + (max - min) * r.nextDouble();
		double y = min + (max - min) * r.nextDouble();
		return new Point(x, y);
	}

	protected static List<Point> createRandomPoints(int min, int max, int number) {
		List<Point> points = new ArrayList<Point>(number);
		for (int i = 0; i < number; i++) {
			points.add(createRandomPoint(min, max));
		}
		return points;
	}

	public String toString() {
		return "(" + x + "," + y + ")";
	}

}
