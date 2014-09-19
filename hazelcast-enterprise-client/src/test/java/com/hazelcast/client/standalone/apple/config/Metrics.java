package com.hazelcast.client.standalone.apple.config;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class Metrics extends Thread {
	
	private final String sTitle;
	private final int refresh;
	private final int numUsers;
	private final DateFormat dateFormat;
	private volatile boolean bContinue;

	private volatile long count;
	private volatile long responseTime;

	private volatile long totalCount;
	private volatile long totalTime;
	private volatile long startTime;
	private final Map<String, AtomicLong> responseTimeDistribution;

	private static final String _LT_1MS = "<1ms";
	private static final String _1MS_2MS = "1-2ms";
	private static final String _2MS_5MS = "2-5ms";
	private static final String _5MS_10MS = "5-10ms";
	private static final String _10MS_15MS = "10-15ms";
	private static final String _GT_15MS = ">15ms";
	private static final int _1MS = 1000;
	private static final int _2MS = 2000;
	private static final int _5MS = 5000;
	private static final int _10MS = 10000;
	private static final int _15MS = 15000;
	
	public Metrics(String title, int interval, int users) {
		sTitle = title;
		refresh = interval * 1000;
		numUsers = users;

		dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

		bContinue = false;
		count = 0;
		responseTime = 0;

		totalCount = 0;
		totalTime = 0;
		startTime = 0;
		responseTimeDistribution = new HashMap<String, AtomicLong>();
		
		// Add response times
		responseTimeDistribution.put(_LT_1MS, new AtomicLong());
		responseTimeDistribution.put(_1MS_2MS, new AtomicLong());
		responseTimeDistribution.put(_2MS_5MS, new AtomicLong());
		responseTimeDistribution.put(_5MS_10MS, new AtomicLong());
		responseTimeDistribution.put(_10MS_15MS, new AtomicLong());
		responseTimeDistribution.put(_GT_15MS, new AtomicLong());
	}

	public void updateValue(long time) {
		synchronized (this) {
			count++;
			long currentResponse = time / 1000;
			responseTime += currentResponse;

			totalCount++;
			totalTime += currentResponse;
			
			String key = _GT_15MS;
			if (currentResponse < _1MS) {
				key = _LT_1MS;
			} else if (currentResponse < _2MS) {
				key = _1MS_2MS;
			} else if (currentResponse < _5MS) {
				key = _2MS_5MS;
			} else if (currentResponse < _10MS) {
				key = _5MS_10MS;
			} else if (currentResponse < _15MS) {
				key = _10MS_15MS;
			}
			responseTimeDistribution.get(key).incrementAndGet();
		}
	}

	public void close() {
		bContinue = false;
		if (totalCount > 0)
			System.out.println(dateFormat.format(new Date()) + ", " + sTitle + " - Total Count: " + totalCount + ", Total Time: " + totalTime / 1000 + ", Total Users: " + numUsers
					+ ", Test Duration: " + (System.currentTimeMillis() - startTime) + ", Avg Response Time micro-secs (total time / total count): " + (totalTime / totalCount)
					+ ", Calculated Throughput(based on response time and number of users - (1 second * num users / avg. response time)): "
					+ (long) Math.floor(1000 * numUsers * totalCount * 1000 / totalTime) + " Response time distribution: " + responseTimeDistribution.toString());
	}

	@Override
	public void run() {
		bContinue = true;
		System.out.println("Title, Date, Elapsed Time, Hits, Time (micro-secs), Res. Time (micro-secs), Throughput");
		startTime = System.currentTimeMillis();
		while (bContinue) {
			synchronized (this) {
				if (count > 0) {
					responseTime = (responseTime == 0 ? 1 : responseTime);
					System.out.println(dateFormat.format(new Date()) + ", " + sTitle + ", " + (System.currentTimeMillis() - startTime) + ", " + count + ", " + responseTime + ", "
							+ (responseTime / count) + ", " + (long) Math.floor(1000 * numUsers * 1000 * count / responseTime));
					count = 0;
					responseTime = 0;
				}
			}
			try {
				Thread.sleep(refresh);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}
}
