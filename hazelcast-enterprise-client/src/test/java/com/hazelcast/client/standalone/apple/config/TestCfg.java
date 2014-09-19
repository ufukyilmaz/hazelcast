package com.hazelcast.client.standalone.apple.config;

import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class TestCfg {

	private static final TestCfg CONFIGURATION = new TestCfg();
	public static final int TEST_PUT = 1;
	public static final int TEST_GET = 2;
	public static final int TEST_PUT_GET = 3;
	public static final int TEST_PUT_GET_SIMULTANEOUS = 4;
	public static final int TEST_PUT_GET_DELETE = 5;

	private int numUsers;
	private int durationInMillis;
	private int testType;
	private int dataSize;
	private long ttl;
	private long maxSequence;
	private String cacheName;
	private int putPercent;
	private int getPercent;
	private boolean isGetOnly;
	private List<String> ipList;
	private boolean audit;
	private int metricsRefreshInterval;
	private int systemMonitorInterval;
	private int statsMonitorInterval;
	private int sleepBeforeGet;

	private TestCfg() {
		Properties properties = new Properties();
		try {
            URL url = new URL("http://storage.googleapis.com/hazelcast/apple-test.properties?t=" + System.currentTimeMillis());
            properties.load(url.openStream());
			// Specifies total number of virtual user
			numUsers = Integer.parseInt(properties.getProperty("num_users", "1"));
			// Specifies duration to run per virtual user
			durationInMillis = (Integer.parseInt(properties.getProperty("duration", "10"))) * 1000;
			// Test type
			testType = Integer.parseInt(properties.getProperty("test_type", "3"));
			// Data size
			dataSize = Integer.parseInt(properties.getProperty("data_size", "4096"));
			// TTL
			ttl = Long.parseLong(properties.getProperty("time_to_live", "120"));
			// Cache Name
			cacheName = System.getProperty("cache.name", properties.getProperty("cache_name"));
			// Put Get distribution
			String distribution = properties.getProperty("put_get_distribution");
			String[] dist = distribution.split("-");
			putPercent = Integer.parseInt(dist[0]);
			getPercent = Integer.parseInt(dist[1]);

			isGetOnly = getPercent == 100 || testType == TEST_GET;

			// Max Sequence for Get, used when Get % is 100% or is Get only test
			maxSequence = Long.parseLong(properties.getProperty("max_seq", "10000"));
			
			// IP Addresses 
			ipList = Arrays.asList(properties.getProperty("put_client_ips").split(","));

			// Add to audit logs
			audit = Boolean.parseBoolean(properties.getProperty("use_audit"));

			// Metrics refresh interval
			metricsRefreshInterval = Integer.parseInt(properties.getProperty("metric_refresh_interval", "1"));
			// System Monitor interval
			systemMonitorInterval = Integer.parseInt(properties.getProperty("system_monitor_interval", "1"));
			// Stats monitor interval
			statsMonitorInterval = Integer.parseInt(properties.getProperty("stats_monitor_interval", "1"));

			// Sleep before Get in PutGet
			sleepBeforeGet = Integer.parseInt(properties.getProperty("sleepBeforeGet", "0"));

			SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss z");
			Date startTime = dateFormat.parse(properties.getProperty("start_time"));
			long sleepTime = startTime.getTime() - System.currentTimeMillis();
			Thread.sleep(sleepTime > 0 ? sleepTime : 0);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public int getNumUsers() {
		return numUsers;
	}

	public int getDurationInMillis() {
		return durationInMillis;
	}

	public static TestCfg getInstance() {
		return CONFIGURATION;
	}

	public int getTestType() {
		return testType;
	}

	public int getDataSize() {
		return dataSize;
	}

	public long getTTLValue() {
		return ttl;
	}

	public String getCacheName() {
		return cacheName;
	}

	public int putPercent() {
		return putPercent;
	}

	public int getPercent() {
		return getPercent;
	}

	public long getMaxSequence() {
		return maxSequence;
	}

	public boolean isGetOnly() {
		return isGetOnly;
	}

	public List<String> getIpList() {
		return ipList;
	}

	public boolean useAudit() {
		return audit;
	}

	public int getMetricRefreshInterval() {
		return metricsRefreshInterval;
	}
	
	public int getSystemMonitorInterval() {
		return systemMonitorInterval;
	}
	
	public int getStatsMonitorInterval() {
		return statsMonitorInterval;
	}
	
	public int getSleepBeforeGetTime() {
		return sleepBeforeGet;
	}
}
