package com.hazelcast.client.standalone.apple.monitor;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.lang.management.ManagementFactory;
import java.util.logging.Level;

public class SysResMon {
	
	protected static ILogger monitorLogger = Logger.getLogger("MonitorLog");
	
	private String key;
	private StringBuilder sb = new StringBuilder();
	private long lUptime = 0;
	private long lProcessCPUTime = 0;
	private long lPreviousUptime = 0;
	private long lPreviousProcessCPUTime = 0;
	private int iAvailableProcessors = 0;
	private int cpuUsage = -1;
	
	private long lTotalPhysicalMemory = 0; 
	private long lFreePhysicalMemory = 0; 
	private long lCommitedVirtualMemory = 0;
	private long lTotalSwapSpace = 0;
	private long lFreeSwapSpace = 0;

	public SysResMon(String key) {
		this.key = key;
		ManagementFactory.getThreadMXBean().resetPeakThreadCount();
	}
	
	public void checkAvailability() {
		
		lUptime = ManagementFactory.getRuntimeMXBean().getUptime();
		
		if (ManagementFactory.getOperatingSystemMXBean() instanceof com.sun.management.OperatingSystemMXBean) {
			com.sun.management.OperatingSystemMXBean osMBean = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
			iAvailableProcessors = osMBean.getAvailableProcessors();
			lProcessCPUTime = osMBean.getProcessCpuTime();
			if (lPreviousUptime > 0)
			{
				final long elapsedCpu = lProcessCPUTime - lPreviousProcessCPUTime;
				final long elapsedTime = lUptime - lPreviousUptime;
				final long divisor = elapsedTime * 10000 * iAvailableProcessors;
				if (divisor > 0)
				{
					cpuUsage = Math.round(Math.min(100F, elapsedCpu /(divisor * 1f)));				
				}
			}
			lPreviousUptime = lUptime;
			lPreviousProcessCPUTime = lProcessCPUTime;
			
			lFreeSwapSpace = osMBean.getFreeSwapSpaceSize();
			lTotalSwapSpace = osMBean.getTotalSwapSpaceSize();
			
			lFreePhysicalMemory = osMBean.getFreePhysicalMemorySize();
			lTotalPhysicalMemory = osMBean.getTotalPhysicalMemorySize();
			lCommitedVirtualMemory = osMBean.getCommittedVirtualMemorySize();
		}

        if (monitorLogger.isLoggable(Level.INFO)) {
			
			sb.setLength(0);
			
			sb.append("TYPE=system_monitor KEY='");
			sb.append(this.key);
			sb.append('\'');
			
			if (cpuUsage != -1) {
				sb.append(" CPU_USAGE=");
				sb.append(cpuUsage);
			}
			
			sb.append(" CPU_PROCESSORS=");
			sb.append(iAvailableProcessors);

			sb.append(" LOAD_AVG=");
			sb.append(ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage());
			
			// Virtual memory
			sb.append(" FREE_VIR_MEM_BYTES=");
			sb.append(lFreeSwapSpace);

			sb.append(" COMM_VIR_MEM_BYTES=");
			sb.append(lCommitedVirtualMemory);

			sb.append(" TOTAL_VIR_MEM_BYTES=");
			sb.append(lTotalSwapSpace);

			// Physical memory
			sb.append(" FREE_PHY_MEM_BYTES=");
			sb.append(lFreePhysicalMemory);

			sb.append(" TOTAL_PHY_MEM_BYTES=");
			sb.append(lTotalPhysicalMemory);
			
			// Heap memory
			sb.append(" USED_HEAP_MEM_BYTES=");
			sb.append(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());

			sb.append(" MAX_HEAP_MEM_BYTES=");
			sb.append(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax());

			sb.append(" COMM_HEAP_MEM_BYTES=");
			sb.append(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getCommitted());
			
			// Non-Heap memory
			sb.append(" USED_NONHEAP_MEM_BYTES=");
			sb.append(ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage().getUsed());

			sb.append(" MAX_NONHEAP_MEM_BYTES=");
			sb.append(ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage().getMax());

			sb.append(" COMM_NONHEAP_MEM_BYTES=");
			sb.append(ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage().getCommitted());
			
			// Threads
			sb.append(" LIVE_THREADS=");
			sb.append(ManagementFactory.getThreadMXBean().getThreadCount());

			sb.append(" PEAK_THREADS=");
			sb.append(ManagementFactory.getThreadMXBean().getPeakThreadCount());

			sb.append(" DAEMON_THREADS=");
			sb.append(ManagementFactory.getThreadMXBean().getDaemonThreadCount());

			monitorLogger.info(sb.toString());
		}

	}
}
