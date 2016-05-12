package com.isikerhan.sparkexamples.ml.kmeans;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.function.PairFunction;

import com.isikerhan.sparkexamples.ml.kmeans.Cluster;
import com.isikerhan.sparkexamples.ml.math.Vector;
import com.isikerhan.sparkexamples.ml.math.distance.DistanceFunction;

import scala.Tuple2;

public class ClusterMappingPairer implements Serializable, PairFunction<Vector<?>, Vector<?>, Cluster> {

	private static final long serialVersionUID = 1L;

	private List<Cluster> clusterList;
	private DistanceFunction distanceFunction;

	public ClusterMappingPairer(List<Cluster> clusterList, DistanceFunction distanceFunction) {
		super();
		this.clusterList = clusterList;
		this.distanceFunction = distanceFunction;
	}

	@Override
	public Tuple2<Vector<?>, Cluster> call(Vector<?> v) throws Exception {
		double minDistance = Double.MAX_VALUE;
		Cluster c = null;

		for (int i = 0; i < clusterList.size(); i++) {
			Cluster current = clusterList.get(i);
			double dist = distanceFunction.distance(v, current.getCentroid());
			if (dist < minDistance) {
				minDistance = dist;
				c = current;
			}
		}

		return new Tuple2<>(v, c);
	}

}
