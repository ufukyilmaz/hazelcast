package com.hazelcast.internal.memory;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.memory.NativeOutOfMemoryError;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.memory.MemorySize.toPrettyString;
import static java.lang.String.format;

/**
 * Adjust memory allocations based on their types. Three types of
 * allocations exist: Metadata, regular data and hot restart data.
 *
 * Multiple threads can access this class concurrently.
 *
 * @see AllocationType
 */
@SuppressWarnings("checkstyle:multiplevariabledeclarations")
public class MemoryAdjuster {
    /**
     * Makes this class aware whether hot restart loading is in progress.
     */
    public static final ThreadLocal<Boolean> HOT_RESTART_LOADING_IN_PROGRESS
            = ThreadLocal.withInitial(() -> false);

    // package private for testing
    static final String PROP_SYSTEM_MEMORY_ENABLED = "hazelcast.internal.system.memory.expansion.enabled";
    static final String DEFAULT_SYSTEM_MEMORY_ENABLED = "true";

    private static final long LOGGING_PERIOD = TimeUnit.MINUTES.toNanos(1);
    private static final ILogger LOGGER = Logger.getLogger(MemoryAdjuster.class);

    private final boolean systemMemoryEnabled = isSystemMemoryEnabled();
    private final NativeMemoryStats stats;
    private final AtomicLong metadataThresholdLastLogTime = new AtomicLong();
    private final AtomicLong systemMemoryExpansionLastLogTime = new AtomicLong();

    public MemoryAdjuster(NativeMemoryStats stats) {
        this.stats = stats;
    }

    private boolean isSystemMemoryEnabled() {
        return Boolean.valueOf(System.getProperty(PROP_SYSTEM_MEMORY_ENABLED,
                DEFAULT_SYSTEM_MEMORY_ENABLED));
    }

    public void adjustMetadataMemory(long requested) {
        // maxMetadata is used as a threshold before raising a warning
        long limit = stats.getMaxMetadata();
        long used = stats.getUsedMetadata();

        if (used + requested > limit && shouldLogNow(metadataThresholdLastLogTime)) {
            LOGGER.warning(createMessage(format("Exceeded initial metadata threshold of %s!",
                    toPrettyString(limit)), requested, stats));
        }

        do {
            if (tryIncrementCommitted(requested, AllocationType.METADATA_ALLOCATION)) {
                return;
            }
        } while (tryIncrementMaxNative(requested, AllocationType.METADATA_ALLOCATION));

        throwNativeOOME("Metadata allocation request cannot be satisfied! ", requested, stats);
    }

    private boolean tryIncrementCommitted(final long requested,
                                          final AllocationType allocationType) {
        if (requested <= 0) {
            return false;
        }

        long committed, nextCommitted;
        do {
            committed = stats.getCommittedNative();
            nextCommitted = committed + requested;

            if (nextCommitted > nextCommitThreshold(allocationType, stats)) {
                return false;
            }
        } while (!stats.casCommittedNative(committed, nextCommitted));

        return true;
    }

    private boolean tryIncrementMaxNative(final long requested,
                                          final AllocationType allocationType) {
        if (!systemMemoryEnabled) {
            return false;
        }

        if (shouldLogNow(systemMemoryExpansionLastLogTime)) {
            LOGGER.warning(createMessage(format("Expanding into system memory to allocate %s.",
                    allocationType.toPrintable()), requested, stats));
        }

        // Adjust requested size to next multiple of page size to relax
        // contention for free-space-asking during metadata allocations
        final long adjustedSize = allocationType == AllocationType.METADATA_ALLOCATION
                ? fitRequestedToNextPageSize(requested, ((PooledNativeMemoryStats) stats).getPageSize()) : requested;

        long maxNative, nextMaxNative;
        do {
            long freePhysical = stats.getFreePhysical();
            if (requested > freePhysical) {
                return false;
            }

            maxNative = stats.getMaxNative();
            nextMaxNative = maxNative + (freePhysical >= adjustedSize ? adjustedSize : requested);
        } while (!stats.casMaxNative(maxNative, nextMaxNative));

        return true;
    }

    // package private for testing
    static long fitRequestedToNextPageSize(final long requested, final long pageSize) {
        long nextPageSize = requested + pageSize;
        return nextPageSize - (nextPageSize % pageSize);
    }

    public void adjustDataMemory(final long requested) {
        // if data-space allocation is asked for hot restart data loading
        if (isHotRestartLoadingInProgress()) {
            do {
                if (tryIncrementCommitted(requested, AllocationType.HOT_RESTART_DATA_ALLOCATION)) {
                    return;
                }
            } while (tryIncrementMaxNative(requested, AllocationType.HOT_RESTART_DATA_ALLOCATION));

            throwNativeOOME("HotRestart data loading cannot be completed! "
                    + "Not enough contiguous memory available!", requested, stats);
        }

        // if data-space allocation is asked for regular data allocation.
        if (tryIncrementCommitted(requested, AllocationType.REGULAR_DATA_ALLOCATION)) {
            return;
        }

        throwNativeOOME("Data allocation request cannot be satisfied! "
                + "Not enough contiguous memory available!", requested, stats);
    }

    private static boolean isHotRestartLoadingInProgress() {
        return HOT_RESTART_LOADING_IN_PROGRESS.get();
    }

    private static long nextCommitThreshold(AllocationType allocationType, NativeMemoryStats stats) {
        return allocationType == AllocationType.REGULAR_DATA_ALLOCATION
                ? stats.getConfiguredMaxNative() : stats.getMaxNative();
    }

    private static void throwNativeOOME(String userGivenMsg, long requested, NativeMemoryStats stats) {
        throw new NativeOutOfMemoryError(createMessage(userGivenMsg, requested, stats));
    }

    private static String createMessage(String userGivenMsg, long requested, NativeMemoryStats stats) {
        String message = userGivenMsg
                + " Cannot allocate " + toPrettyString(requested) + "!"
                + " [MaxNative: " + toPrettyString(stats.getMaxNative())
                + ", CommittedNative: " + toPrettyString(stats.getCommittedNative())
                + ", UsedNative: " + toPrettyString(stats.getUsedNative());

        if (stats instanceof PooledNativeMemoryStats) {
            message = message
                    + ", UsedMetadata: " + toPrettyString(stats.getUsedMetadata())
                    + ", MaxMetadata: " + toPrettyString(stats.getMaxMetadata());
        }

        return message + ", FreePhysical: " + toPrettyString(stats.getFreePhysical()) + "]";
    }

    private static boolean shouldLogNow(AtomicLong lastLogTime) {
        if (!LOGGER.isWarningEnabled()) {
            return false;
        }

        long now = System.nanoTime();
        long lastLogged = lastLogTime.get();
        if (now - lastLogged >= LOGGING_PERIOD) {
            return lastLogTime.compareAndSet(lastLogged, now);
        }

        return false;
    }

    /**
     * Types of memory allocation requests.
     */
    private enum AllocationType {
        METADATA_ALLOCATION() {
            @Override
            String toPrintable() {
                return "metadata";
            }
        },

        REGULAR_DATA_ALLOCATION() {
            @Override
            String toPrintable() {
                return "data";
            }
        },

        HOT_RESTART_DATA_ALLOCATION() {
            @Override
            String toPrintable() {
                return "hot restart data";
            }
        };

        abstract String toPrintable();
    }
}
