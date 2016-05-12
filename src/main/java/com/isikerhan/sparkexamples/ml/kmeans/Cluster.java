package com.isikerhan.sparkexamples.ml.kmeans;

import java.io.Serializable;

import com.isikerhan.sparkexamples.ml.math.Vector;

public class Cluster implements Serializable{

	private static final long serialVersionUID = 1L;
	// private List<Vector<?>> elements;
	private Vector<Double> centroid;
	private int clusterNo;

	public Cluster(Vector<Double> initialCentroid, int clusterNo) {

		centroid = initialCentroid;
		// elements = new ArrayList<>();
		this.clusterNo = clusterNo;
	}

	// public List<Vector<?>> getElements() {
	// return elements;
	// }
	//
	// public void addElement(Vector<?> element) {
	// elements.add(element);
	// }

	public Vector<Double> getCentroid() {
		return centroid;
	}

	public void setCentroid(Vector<Double> centroid) {
		this.centroid = centroid;
	}

	public int getClusterNo() {
		return clusterNo;
	}

	public void setClusterNo(int clusterNo) {
		this.clusterNo = clusterNo;
	}
	
	@Override
	public String toString() {
		return String.format("Cluster %d", clusterNo);
	}

	@Override
	public boolean equals(Object obj) {

		if (!(obj instanceof Cluster))
			return false;
		
		Cluster other = (Cluster) obj;
		return other.clusterNo == clusterNo;
	}
	
	@Override
	public int hashCode() {
		return clusterNo;
	}
}