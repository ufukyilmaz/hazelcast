//package com.hazelcast.client.standalone.apple.monitor;
//
//import com.hazelcast.client.standalone.apple.test.HzEngine;
//import com.hazelcast.logging.ILogger;
//import com.hazelcast.logging.Logger;
//
//import java.util.logging.Level;
//
//public class StatsMon {
//
//	protected static ILogger monitorLogger = Logger.getLogger("MonitorLog");
//	private StringBuilder builder = new StringBuilder();
//	private String key;
//
//	public StatsMon(String key) {
//		this.key = key;
//	}
//
//	public void collectStats() {
//		if (monitorLogger.isLoggable(Level.INFO)) {
//			builder.setLength(0);
//			builder.append("Key=");
//			builder.append(key);
//			HzEngine.getInstance().fetchCacheStats(builder);
//			monitorLogger.info(builder.toString());
//
//			builder.setLength(0);
//			builder.append("Key=");
//			builder.append(key);
//			HzEngine.getInstance().fetchNearCacheStats(builder);
//			monitorLogger.info(builder.toString());
//		}
//	}
//
//	public static void printFinalStats(String key) {
//        if (monitorLogger.isLoggable(Level.INFO)) {
//			StringBuilder builder = new StringBuilder();
//			builder.append("Key=");
//			builder.append(key);
//			HzEngine.getInstance().fetchCacheStats(builder);
//			monitorLogger.info(builder.toString());
//
//			builder.setLength(0);
//			builder.append("Key=");
//			builder.append(key);
//			HzEngine.getInstance().fetchNearCacheStats(builder);
//			monitorLogger.info(builder.toString());
//		}
//	}
//}
