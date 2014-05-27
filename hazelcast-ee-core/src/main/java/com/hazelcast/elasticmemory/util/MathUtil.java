package com.hazelcast.elasticmemory.util;

public final class MathUtil {

	public static int divideByAndCeilToInt(double d, int k) {
		return (int) Math.ceil(d / k);
	}
	
	public static long divideByAndCeilToLong(double d, int k) {
		return (long) Math.ceil(d / k);
	}
	
	public static int divideByAndRoundToInt(double d, int k) {
		return (int) Math.rint(d / k);
	}
	
	public static long divideByAndRoundToLong(double d, int k) {
		return (long) Math.rint(d / k);
	}

	public static boolean isPowerOf2(long x) {
		if(x <= 0) return false;
		return (x & (x - 1)) == 0;
	}
	
	private MathUtil() {}
}
