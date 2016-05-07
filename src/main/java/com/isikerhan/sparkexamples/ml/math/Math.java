package com.isikerhan.sparkexamples.ml.math;

import static java.lang.Math.sqrt;
import static java.lang.Math.pow;

public final class Math {

	private Math(){
	}
	
	/**
	 * Returns the <code>n</code>th root of the first argument.
	 * @param a the base.
	 * @param n the root degree.
	 * @return <code>n</code>th root of the base.
	 */
	public static double root(double a, double n) {
		
		if(new Double(1.0).equals(n))
			return a;
		if (new Double(2.0).equals(n))
			return sqrt(a);
		return pow(a, 1 / n);
	}
}
