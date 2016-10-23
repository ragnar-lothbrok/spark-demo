package com.spark.kmeans.brainwaves;

public class Point {

	private double x = 0;
	private double y = 0;
	private String name;
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

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Point(double x, double y, String name) {
		super();
		this.x = x;
		this.y = y;
		this.name = name;
	}

	// Calculates the distance between two points.
	static double distance(Point p, Point centroid) {
		return Math.sqrt(Math.pow((centroid.getY() - p.getY()), 2)/* + Math.pow((centroid.getX() - p.getX()), 2)*/);
	}

	@Override
	public String toString() {
		return "Point [x=" + x + ", y=" + y + ", name=" + name + ", cluster_number=" + cluster_number + "]";
	}

}
