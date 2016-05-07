package com.isikerhan.sparkexamples.ml.kmeans;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import com.isikerhan.sparkexamples.ml.math.Vector;
import com.isikerhan.sparkexamples.ml.math.distance.DistanceFunction;
import com.isikerhan.sparkexamples.ml.math.distance.EuclideanDistance;

import scala.Tuple2;

public class KMeans {

	private JavaSparkContext sparkContext;
	private JavaRDD<Cluster> clusters;
	private JavaPairRDD<Cluster, Vector<?>> clusterIndexes;
	private int k;
	private JavaRDD<Vector<?>> dataSource;
	protected DistanceFunction distanceFunction = new EuclideanDistance();

	public KMeans(int k, JavaRDD<Vector<?>> dataSource, JavaSparkContext sparkContext) {

		this.sparkContext = sparkContext;
		this.k = k;
		this.dataSource = dataSource;
	}

	public void initCentroids() {

		List<Cluster> clusterList = new ArrayList<>();

		for (int i = 0; i < k; i++) {

			Vector<?> v = dataSource.sample(false, 1).first();
			clusterList.add(new Cluster(v.toDoubleVector(), i));
		}

		clusters = sparkContext.parallelize(clusterList);
	}

	public boolean iterate() {

		PairFunction<Vector<?>, Cluster, Vector<?>> pairer = (Vector<?> v) -> {

			Comparator<Cluster> comp = (Cluster c1, Cluster c2) -> Double.compare(
					distanceFunction.distance(c1.getCentroid(), v), distanceFunction.distance(c2.getCentroid(), v));
			Cluster c = clusters.min(comp);
			return new Tuple2<Cluster, Vector<?>>(c, v);
		};
		JavaPairRDD<Cluster, Vector<?>> newClusterIndexes = dataSource.mapToPair(pairer);

		boolean changed = !newClusterIndexes.join(clusterIndexes).map(t -> t._2._1.equals(t._2._2)).reduce((a, b) -> a && b);

		if (changed)
			recalculateClusters();
		return changed;
	}

	private void recalculateClusters() {

		JavaPairRDD<Cluster, Integer> counts = clusterIndexes.mapToPair(t -> new Tuple2<>(t._1, 1))
				.reduceByKey((a, b) -> a + b);

		JavaPairRDD<Cluster, Vector<?>> sums = clusterIndexes.reduceByKey((a, b) -> a.add(b));

		JavaPairRDD<Cluster, Vector<?>> centroids = counts.join(sums)
				.mapToPair(t -> new Tuple2<>(t._1, t._2._2.divide(t._2._1.doubleValue())));

		clusters = centroids.map(t -> {
			Cluster c = t._1();
			c.setCentroid(t._2.toDoubleVector());
			return c;
		});
		// JavaPairRDD<Cluster, Vector<Double>> sums = clusterIndexes
		// clusterIndexes.mapToPair(pairer);
	}

	// public double sumOfSquaredErrors(){
	//
	// double sum = 0.0f;
	//
	// for(Cluster c : getClusters()) {
	// Vector<Double> centroid = c.getCentroid();
	// for(Vector<?> v : c.getElements())
	// sum += Math.pow(distanceFunction.distance(centroid, v), 2.0);
	// }
	//
	// return sum;
	// }

	// public Cluster[] getClusters() {
	// return clusters;
	// }
	//
	// public void setClusters(Cluster[] clusters) {
	// this.clusters = clusters;
	// }

	// @Override
	// public String toString() {
	// String nl = System.getProperty("line.separator");
	// StringBuilder sb = new StringBuilder();
	// for(int i = 0; i < getClusters().length; i++) {
	// sb.append(String.format("********** Cluster %d **********" + nl, i));
	// Cluster c = getClusters()[i];
	// for(Vector<?> v : c.getElements())
	// sb.append(v.toString() + nl);
	// sb.append(nl);
	// }
	//
	// sb.append(String.format("Sum of squared errors: %.2f.",
	// sumOfSquaredErrors()));
	// return sb.toString();
	// }
}
