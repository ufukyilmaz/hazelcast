//package com.hazelcast.client.standalone.apple.monitor;
//
//import com.hazelcast.core.HazelcastInstance;
//import com.hazelcast.logging.ILogger;
//import com.hazelcast.logging.Logger;
//
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.Future;
//import java.util.concurrent.ScheduledThreadPoolExecutor;
//import java.util.concurrent.TimeUnit;
//
//public class Scheduler {
//
//	private static ScheduledThreadPoolExecutor stpe = new ScheduledThreadPoolExecutor(5);
//	private static Map<String, Future<?>> scheduledMonitors = new ConcurrentHashMap<String, Future<?>>();
//	private static ILogger logger = Logger.getLogger(Scheduler.class);
//
//	public static void scheduleSystemMonitor(final HazelcastInstance instance, String ipAddress, int interval) {
//		if (scheduledMonitors.containsKey("SystemMonitor")) {
//			return;
//		}
//		final SysResMon monitorBean = new SysResMon(ipAddress);
//		final Runnable siteCheck = new Runnable() {
//			@Override
//			public void run() {
//				try {
//					monitorBean.checkAvailability();
//				} catch (Exception e) {
//					logger.severe("Error occurred while monitoring system resources.", e);
//				}
//			}
//		};
//		if (logger.isFinestEnabled())
//			logger.finest("Monitoring system resources every " + interval + " secs");
//		Future<?> schedulerFuture = stpe.scheduleWithFixedDelay(siteCheck, interval, interval, TimeUnit.SECONDS);
//		scheduledMonitors.put("SystemMonitor", schedulerFuture);
//	}
//
//	public static void scheduleHcStatsMonitor(String ipAddress, int interval) {
//		if (scheduledMonitors.containsKey("StatsMonitor")) {
//			return;
//		}
//
//		final StatsMon monitor = new StatsMon(ipAddress);
//		final Runnable siteCheck = new Runnable() {
//			@Override
//			public void run() {
//				try {
//					monitor.collectStats();
//				} catch (Exception e) {
//					logger.severe("Error occurred while fetching server stats.", e);
//				}
//			}
//		};
//		if (logger.isFinestEnabled())
//			logger.finest("Monitoring server stats every " + interval + " secs");
//		Future<?> schedulerFuture = stpe.scheduleWithFixedDelay(siteCheck, interval, interval, TimeUnit.SECONDS);
//		scheduledMonitors.put("StatsMonitor", schedulerFuture);
//	}
//
////	public static void scheduleHazelcastHealthMonitor(HazelcastInstanceImpl instance, int interval) {
////		HealthMonitor monitor = new HealthMonitor(instance, HealthMonitorLevel.NOISY, interval);
////		Future<?> schedulerFuture = stpe.scheduleWithFixedDelay(monitor, interval, interval, TimeUnit.SECONDS);
////		scheduledMonitors.put("HealthMonitor", schedulerFuture);
////	}
//
//	public static void shutdownAllMonitors() {
//		for (Future<?> future : scheduledMonitors.values()) {
//			future.cancel(false);
//		}
//		stpe.shutdown();
//	}
//
//}
