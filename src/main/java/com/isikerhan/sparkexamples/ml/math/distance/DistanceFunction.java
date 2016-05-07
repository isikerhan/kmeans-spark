package com.isikerhan.sparkexamples.ml.math.distance;

import com.isikerhan.sparkexamples.ml.math.Vector;

public interface DistanceFunction {

	double distance(Vector<?> p1, Vector<?> p2);
}
