package com.hazelcast.elasticmemory.util;

public final class MathUtil {

	public static int divideByAndCeil(double d, int k) {
		return (int) Math.ceil(d / k);
	}
	
	public static long divideByAndCeil(double d, long k) {
		return (long) Math.ceil(d / k);
	}
	
	public static int divideByAndRound(double d, int k) {
		return (int) Math.rint(d / k);
	}
	
	public static long divideByAndRound(double d, long k) {
		return (long) Math.rint(d / k);
	}

	public static boolean isPowerOf2(long x) {
		if(x <= 0) return false;
		return (x & (x - 1)) == 0;
	}
	
	private MathUtil() {}
}
