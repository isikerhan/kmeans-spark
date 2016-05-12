package com.isikerhan.sparkexamples.ml.kmeans;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.isikerhan.sparkexamples.ml.math.Vector;
import com.isikerhan.sparkexamples.ml.math.distance.DistanceFunction;
import com.isikerhan.sparkexamples.ml.math.distance.EuclideanDistance;

import scala.Tuple2;

public class KMeans implements Serializable{

	private static final long serialVersionUID = 1L;

	private JavaSparkContext sparkContext;
	private JavaRDD<Cluster> clusters;
	private JavaPairRDD<Vector<?>, Cluster> clusterMapping;
	private int k;
	private JavaRDD<Vector<?>> dataSource;
	protected DistanceFunction distanceFunction = new EuclideanDistance();

	public KMeans(int k, JavaRDD<Vector<?>> dataSource, JavaSparkContext sparkContext) {

		this.sparkContext = sparkContext;
		this.k = k;
		this.dataSource = dataSource;
	}

	public JavaPairRDD<Vector<?>, Cluster> getClusterMapping() {
		return clusterMapping;
	}
	
	public void setClusterMapping(JavaPairRDD<Vector<?>, Cluster> clusterMapping) {
		this.clusterMapping = clusterMapping;
	}

	public JavaRDD<Cluster> getClusters() {
		return clusters;
	}

	public void setClusters(JavaRDD<Cluster> clusters) {
		this.clusters = clusters;
	}

	public void initCentroids() {

		List<Cluster> clusterList = new ArrayList<>();

		Random rand = new Random();
		
		for (int i = 0; i < k; i++) {

			Vector<?> v = dataSource.takeSample(false, 1, rand.nextLong()).get(0);
			clusterList.add(new Cluster(v.toDoubleVector(), i));
		}

		clusters = sparkContext.parallelize(clusterList);
		System.out.println("Clusters initialized");
	}

	public boolean iterate() {
		
		clusters = clusters.cache();
		List<Cluster> clusterList = clusters.collect();
		
		ClusterMappingPairer pairer = new ClusterMappingPairer(clusterList, distanceFunction);
		JavaPairRDD<Vector<?>, Cluster> newClusterMapping = dataSource.mapToPair(pairer);
		clusters = clusters.unpersist();
		
		boolean changed = (clusterMapping == null) ? true : !newClusterMapping.join(clusterMapping).map(t -> t._2()._1().equals(t._2()._2()))
				.reduce((a, b) -> a && b);

		clusterMapping = newClusterMapping;
		if (changed)
			recalculateClusters();
		return changed;
	}

	private void recalculateClusters() {
		
		JavaPairRDD<Cluster, Vector<?>> reverseClusterMapping = this.clusterMapping.mapToPair(t -> new Tuple2<>(t._2(), t._1()));
		
		JavaPairRDD<Cluster, Integer> counts = reverseClusterMapping.mapToPair(t -> new Tuple2<>(t._1(), 1))
				.reduceByKey((a, b) -> a + b);

		JavaPairRDD<Cluster, Vector<?>> sums = reverseClusterMapping.reduceByKey((a, b) -> a.add(b));
		
		JavaPairRDD<Cluster, Vector<?>> centroids = counts.join(sums)
				.mapToPair(t -> new Tuple2<>(t._1(), t._2()._2().divide(t._2()._1().doubleValue())));
		
		clusters = centroids.map(t -> {
			Cluster c = t._1();
			c.setCentroid(t._2().toDoubleVector());
			return c;
		});
		// JavaPairRDD<Cluster, Vector<Double>> sums = clusterIndexes
		// clusterIndexes.mapToPair(pairer);
	}

	public double sumOfSquaredErrors() {

		Double sum = clusterMapping.map(t -> distanceFunction.distance(t._2().getCentroid(), t._1()))
				.reduce((a, b) -> a + b);

		return sum.doubleValue();
	}
	
}
