package com.isikerhan.sparkexamples.ml.math.distance;

public class EuclideanDistance extends MinkowskiDistance{

	private static final long serialVersionUID = 1L;

	@Override
	protected double getNorm() {
		return 2.0;
	}
}
