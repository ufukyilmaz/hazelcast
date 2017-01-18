package com.hazelcast.memory;

import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import static com.hazelcast.memory.MemoryStatsSupport.freePhysicalMemory;
import static com.hazelcast.memory.MemoryStatsSupport.totalPhysicalMemory;

/**
 * Class which checks if there is enough free native memory for allocation.
 * <p>
 * If disabled, acts as if the check for free memory succeeded.
 */
public final class FreeMemoryChecker {

    /**
     * Name of the property for enabling or disabling the free memory checking.
     */
    public static final String HIDENSITY_CHECK_FREEMEMORY = "hazelcast.hidensity.check.freememory";

    /**
     * Hazelcast XML and system property for setting if free memory checking is enabled.
     */
    public static final HazelcastProperty FREE_MEMORY_CHECKER_ENABLED = new HazelcastProperty(HIDENSITY_CHECK_FREEMEMORY, true);

    private final boolean enabled;

    FreeMemoryChecker() {
        final String enabledStr = FREE_MEMORY_CHECKER_ENABLED.getSystemProperty();
        this.enabled = Boolean.parseBoolean(enabledStr != null ? enabledStr : "true");
    }

    public FreeMemoryChecker(HazelcastProperties properties) {
        this.enabled = properties.getBoolean(FREE_MEMORY_CHECKER_ENABLED);
    }

    /**
     * If enabled and is able to fetch memory statistics via {@link java.lang.management.OperatingSystemMXBean},
     * checks if there is enough free physical memory for the requested number of bytes. If the free memory checker is disabled,
     * acts as if the check succeeded.
     *
     * @param size the number of bytes that need to be allocated
     */
    public void checkFreeMemory(long size) {
        if (!enabled) {
            return;
        }

        long totalMem = totalPhysicalMemory();
        if (totalMem < 0) {
            return;
        }
        long freeMem = freePhysicalMemory();
        if (freeMem < 0) {
            return;
        }
        if (size > freeMem) {
            throw new NativeOutOfMemoryError(String.format("Not enough free physical memory available! "
                    + "Cannot allocate " + MemorySize.toPrettyString(size) + "!"
                    + "Total physical memory: " + MemorySize.toPrettyString(totalMem)
                    + " Free physical memory: " + MemorySize.toPrettyString(freeMem) + "%n"
                    + "Depending on the operating system or virtualization technology the memory check may report incorrect "
                    + "total or free amount of memory.%n"
                    + "You can disable the check by adding the following runtime switch "
                    + "'-Dhazelcast.hidensity.check.freememory=false'"));
        }
    }
}
