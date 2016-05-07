package com.isikerhan.sparkexamples.ml.math.distance;

public class ManhattanDistance extends MinkowskiDistance{

	@Override
	protected double getNorm() {
		return 1.0;
	}

}
