//package com.hazelcast.client.standalone.apple.test;
//
//import com.hazelcast.client.standalone.apple.config.Metrics;
//import com.hazelcast.client.standalone.apple.config.TestCfg;
//import com.hazelcast.client.standalone.apple.monitor.Scheduler;
//import com.hazelcast.client.standalone.apple.monitor.StatsMon;
//import com.hazelcast.core.HazelcastInstance;
//import com.hazelcast.logging.ILogger;
//import com.hazelcast.logging.Logger;
//
//import java.net.Inet4Address;
//import java.net.UnknownHostException;
//import java.util.List;
//import java.util.Random;
//import java.util.concurrent.atomic.AtomicLong;
//
//public class PerfTest {
//
//    static {
//        System.setProperty("hazelcast.version.check.enabled", "false");
//        System.setProperty("hazelcast.socket.bind.any", "false");
//        System.setProperty("java.net.preferIPv4Stack", "true");
//        System.setProperty("hazelcast.multicast.group", "224.35.57.79");
//    }
//
//	private static final ILogger logger = Logger.getLogger(PerfTest.class);
//	private static final ILogger auditLogger = Logger.getLogger("AuditLog");
//
//	public static void main(String[] args) {
//		String ipAddress = "127.0.0.1";
//		try {
//			ipAddress = Inet4Address.getLocalHost().getHostAddress();
//		} catch (UnknownHostException e1) {
//			// Do nothing
//		}
//
//		// Initialize hazelcast instance
//		final HzEngine hazelcastEngine = HzEngine.getInstance();
//		TestCfg testConfiguration = TestCfg.getInstance();
//		hazelcastEngine.init(testConfiguration.getCacheName());
//
//		// Monitor resources
//		final HazelcastInstance hazelcastInstance = hazelcastEngine.getHazelcastInstance();
//		Scheduler.scheduleHcStatsMonitor(ipAddress, testConfiguration.getStatsMonitorInterval());
//		Scheduler.scheduleSystemMonitor(hazelcastInstance, ipAddress, testConfiguration.getSystemMonitorInterval());
//
//		PerfTest test = new PerfTest();
//		try {
//			test.runTest(ipAddress);
//		} catch (Exception e) {
//			logger.severe(e.getMessage(), e);
//		} finally {
//			test.shutdown();
//		}
//	}
//
//	public void runTest(final String ipAddress) {
//		final TestCfg testConfig = TestCfg.getInstance();
//		final HzEngine hazelcastEngine = HzEngine.getInstance();
//		int numUsers = testConfig.getNumUsers();
//		final Thread[] testThreads = new Thread[numUsers];
//		final int testType = testConfig.getTestType();
//		final AtomicLong sequence = new AtomicLong(1);
//		final Random random = new Random();
//
//		StringBuilder sb = new StringBuilder();
//		int putUsers = numUsers;
//		int getUsers = numUsers;
//		int deleteUsers = numUsers;
//		switch (testType) {
//		case TestCfg.TEST_PUT: {
//			sb.append("PutOnly");
//			break;
//		}
//		case TestCfg.TEST_GET: {
//			sb.append("GetOnly");
//			break;
//		}
//		case TestCfg.TEST_PUT_GET_SIMULTANEOUS: {
//			sb.append("PutGetSimultaneous");
//			putUsers = (testConfig.putPercent() * numUsers) / 100;
//			getUsers = (testConfig.getPercent() * numUsers) / 100;
//			break;
//		}
//		case TestCfg.TEST_PUT_GET_DELETE: {
//			sb.append("PutGetDelete");
//			break;
//		}
//		default: {
//			sb.append("PutGet");
//			break;
//		}
//		}
//
//		sb.append(" Users: %USERS%");
//		sb.append(" IP: ");
//		sb.append(ipAddress);
//		sb.append(" Payload: ");
//		sb.append(testConfig.getDataSize());
//		sb.append(" Duration: ");
//		sb.append(testConfig.getDurationInMillis());
//
//		int metricRefreshInterval = testConfig.getMetricRefreshInterval();
//		final Metrics putMetrics = new Metrics(sb.toString().replaceAll("%USERS%", String.valueOf(putUsers)) + ", PUT-METRICS", metricRefreshInterval, putUsers);
//		putMetrics.start();
//		final Metrics getMetrics = new Metrics(sb.toString().replaceAll("%USERS%", String.valueOf(getUsers)) + ", GET-METRICS", metricRefreshInterval, getUsers);
//		getMetrics.start();
//		final Metrics deleteMetrics = new Metrics(sb.toString().replaceAll("%USERS%", String.valueOf(deleteUsers)) + ", DELETE-METRICS", metricRefreshInterval, deleteUsers);
//		deleteMetrics.start();
//		try {
//			final List<String> ipList = testConfig.getIpList();
//			for (int i = 0; i < testThreads.length; i++) {
//				final int vuCount = (i + 1);
//				final int percent = (i * 100) / numUsers;
//				testThreads[i] = new Thread() {
//					@Override
//					public void run() {
//						try {
//							long endTime = System.currentTimeMillis() + testConfig.getDurationInMillis();
//							long ts = 0;
//							switch (testType) {
//							case TestCfg.TEST_PUT: {
//								String data = RndStrUtils.randomAlphanumeric(testConfig.getDataSize());
//								while (System.currentTimeMillis() < endTime) {
//                                    try {
//                                        String key = "HC-Test-Key-" + ipAddress + "-" + sequence.incrementAndGet();
//                                        ts = System.nanoTime();
//                                        hazelcastEngine.put(key, data, testConfig.getTTLValue());
//                                        logStats(ipAddress, testType, key, testConfig.getDataSize(), "PUT", System.nanoTime() - ts, putMetrics);
//                                    } catch (Throwable e) {
//                                        logger.severe(e.getMessage(), e);
//                                    }
//                                }
//								break;
//							}
//							case TestCfg.TEST_GET: {
//								while (System.currentTimeMillis() < endTime) {
//                                    try {
//                                        String key = "HC-Test-Key-" + ipList.get(random.nextInt(ipList.size())) + "-" + nextLong(random, testConfig.getMaxSequence());
//                                        ts = System.nanoTime();
//                                        hazelcastEngine.get(key);
//                                        logStats(ipAddress, testType, key, testConfig.getDataSize(), "GET", System.nanoTime() - ts, getMetrics);
//                                    } catch (Throwable e) {
//                                        logger.severe(e.getMessage(), e);
//                                    }
//                                }
//								break;
//							}
//							case TestCfg.TEST_PUT_GET_SIMULTANEOUS: {
//								if (percent < testConfig.putPercent()) {
//									String data = RndStrUtils.randomAlphanumeric(testConfig.getDataSize());
//									while (System.currentTimeMillis() < endTime) {
//                                        try {
//                                            String key = "HC-Test-Key-" + ipAddress + "-" + sequence.incrementAndGet();
//                                            ts = System.nanoTime();
//                                            hazelcastEngine.put(key, data, testConfig.getTTLValue());
//                                            logStats(ipAddress, testType, key, testConfig.getDataSize(), "PUT", System.nanoTime() - ts, putMetrics);
//                                        } catch (Throwable e) {
//                                            logger.severe(e.getMessage(), e);
//                                        }
//                                    }
//								} else {
//									while (System.currentTimeMillis() < endTime) {
//                                        try {
//                                            long nextSequence = nextLong(random, testConfig.isGetOnly() ? testConfig.getMaxSequence() : sequence.get());
//                                            if (nextSequence == 0) {
//                                                continue;
//                                            }
//                                            String key = "HC-Test-Key-" + ipAddress + "-" + nextSequence;
//                                            ts = System.nanoTime();
//                                            hazelcastEngine.get(key);
//                                            logStats(ipAddress, testType, key, testConfig.getDataSize(), "GET", System.nanoTime() - ts, getMetrics);
//                                        } catch (Throwable e) {
//                                            logger.severe(e.getMessage(), e);
//                                        }
//                                    }
//								}
//								break;
//							}
//							case TestCfg.TEST_PUT_GET_DELETE: {
//								String data = RndStrUtils.randomAlphanumeric(testConfig.getDataSize());
//								while (System.currentTimeMillis() < endTime) {
//                                    try {
//                                        String key = "HC-Test-Key-" + ipAddress + "-" + System.nanoTime();
//                                        ts = System.nanoTime();
//                                        hazelcastEngine.put(key, data, testConfig.getTTLValue());
//                                        logStats(ipAddress, testType, key, testConfig.getDataSize(), "PUT", System.nanoTime() - ts, putMetrics);
//
//                                        ts = System.nanoTime();
//                                        hazelcastEngine.get(key);
//                                        logStats(ipAddress, testType, key, testConfig.getDataSize(), "GET", System.nanoTime() - ts, getMetrics);
//
//                                        ts = System.nanoTime();
//                                        hazelcastEngine.delete(key);
//                                        logStats(ipAddress, testType, key, testConfig.getDataSize(), "DELETE", System.nanoTime() - ts, deleteMetrics);
//                                    } catch (Throwable e) {
//                                        logger.severe(e.getMessage(), e);
//                                    }
//                                }
//								break;
//							}
//							default: {
//								String data = RndStrUtils.randomAlphanumeric(testConfig.getDataSize());
//								while (System.currentTimeMillis() < endTime) {
//                                    try {
//                                        String key = "HC-Test-Key-" + ipAddress + "-" + System.nanoTime();
//                                        ts = System.nanoTime();
//                                        hazelcastEngine.put(key, data, testConfig.getTTLValue());
//                                        logStats(ipAddress, testType, key, testConfig.getDataSize(), "PUT", System.nanoTime() - ts, putMetrics);
//
//                                        Thread.sleep(testConfig.getSleepBeforeGetTime());
//
//                                        ts = System.nanoTime();
//                                        hazelcastEngine.get(key);
//                                        logStats(ipAddress, testType, key, testConfig.getDataSize(), "GET", System.nanoTime() - ts, getMetrics);
//                                    } catch (Throwable e) {
//                                        logger.severe(e.getMessage(), e);
//                                    }
//                                }
//								break;
//							}
//							}
//						} catch (Throwable e) {
//							logger.severe(e.getMessage(), e);
//						}
//					}
//				};
//				testThreads[i].setName("HCTestThread-" + vuCount);
//				testThreads[i].start();
//			}
//			for (int i = 0; i < testThreads.length; i++) {
//				testThreads[i].join();
//			}
//		} catch (Exception e1) {
//			e1.printStackTrace();
//		} finally {
//			deleteMetrics.close();
//			getMetrics.close();
//			putMetrics.close();
//			StatsMon.printFinalStats(ipAddress);
//		}
//	}
//
//	private void shutdown() {
//		Scheduler.shutdownAllMonitors();
//		HzEngine.getInstance().release();
//		//FIXME: Remove after fixed HC#276
//		System.exit(0);
//	}
//
//	private static long nextLong(Random rnd, long n) {
//		long bits, val;
//		do {
//			bits = (rnd.nextLong() << 1) >>> 1;
//			val = bits % n;
//		} while (bits - val + (n - 1) < 0L);
//		return val;
//	}
//
//	private static void logStats(String ipAddress, int type, String key, long payload, String operation, long responseTime, Metrics metric) {
//		metric.updateValue(responseTime);
//		if (TestCfg.getInstance().useAudit()) {
//			StringBuilder builder = new StringBuilder();
//			builder.append("TYPE=");
//			builder.append(type);
//			builder.append(" IP=");
//			builder.append(ipAddress);
//			builder.append(" PLD=");
//			builder.append(payload);
//			builder.append(" OPT=");
//			builder.append(operation);
//			builder.append(" RES_TM=");
//			builder.append(responseTime / 1000);
//			auditLogger.info(builder.toString());
//		}
//	}
//}
