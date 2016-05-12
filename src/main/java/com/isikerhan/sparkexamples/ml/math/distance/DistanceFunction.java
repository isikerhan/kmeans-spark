package com.isikerhan.sparkexamples.ml.math.distance;

import java.io.Serializable;

import com.isikerhan.sparkexamples.ml.math.Vector;

public interface DistanceFunction extends Serializable {

	double distance(Vector<?> p1, Vector<?> p2);
}
