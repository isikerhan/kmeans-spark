package com.isikerhan.sparkexamples.ml.math.distance;

public class EuclideanDistance extends MinkowskiDistance{

	@Override
	protected double getNorm() {
		return 2.0;
	}
}
