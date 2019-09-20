package com.hazelcast.enterprise.wan.impl.replication;

import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanSyncStats;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.util.MapUtil.createConcurrentHashMap;
import static java.util.concurrent.TimeUnit.HOURS;

/**
 * Context class holding objects specific to a WAN synchronization request.
 *
 * @param <T> The type of the WAN synchronizations statistics, can be {@link FullWanSyncStats} and {@link MerkleTreeWanSyncStats}
 */
class WanSyncContext<T extends WanSyncStats> {
    /**
     * We treat a synchronization process stuck if there is no activity for 1 hour.
     * This threshold is used for cleaning up after the stuck synchronization
     * requests.
     *
     * @see #isActive()
     */
    private static final long SYNC_STUCK_THRESHOLD_MILLIS = HOURS.toMillis(1);

    /**
     * The UUID of this synchronization.
     */
    private final UUID uuid;
    /**
     * The name of the maps covered by this synchronization.
     */
    private final Collection<String> mapNames;
    /**
     * The count of {@link WanReplicationEvent} sync events
     * pending replication per map, per partition.
     */
    private final Map<String, Map<Integer, AtomicInteger>> counterMaps;
    /**
     * Map holding the synchronization statistics for each synchronized maps.
     */
    private final Map<String, T> syncStatsMap;
    /**
     * The number of the partitions owned on the given member. Used as a
     * hint for sizing the internal maps.
     */
    private final int ownedPartitions;
    /**
     * Counter counting how many maps are not yet synchronized.
     */
    private final AtomicInteger mapsLeft;
    /**
     * The timestamp of the last access done to this context. Used for
     * cleaning up after stuck synchronization processes.
     */
    private volatile long lastAccess;

    WanSyncContext(UUID uuid, int ownedPartitions, Collection<String> mapNames) {
        access();
        this.uuid = uuid;
        this.ownedPartitions = ownedPartitions;
        this.mapNames = mapNames;
        this.mapsLeft = new AtomicInteger(mapNames.size());
        counterMaps = createConcurrentHashMap(mapNames.size());
        syncStatsMap = createConcurrentHashMap(mapNames.size());
    }

    AtomicInteger getSyncCounter(String mapName, int partitionId) {
        access();
        Map<Integer, AtomicInteger> counterMap = counterMaps
                .computeIfAbsent(mapName, k -> createConcurrentHashMap(ownedPartitions));

        return counterMap.computeIfAbsent(partitionId, k -> new AtomicInteger());
    }

    public UUID getUuid() {
        return uuid;
    }

    T getSyncStats(String mapName) {
        access();
        return syncStatsMap.get(mapName);
    }

    T addSyncStats(String mapName, T syncStats) {
        access();
        return syncStatsMap.put(mapName, syncStats);
    }

    public Collection<String> getMapNames() {
        return mapNames;
    }

    void onMapSynced() {
        access();
        mapsLeft.decrementAndGet();
    }

    boolean isActive() {
        return (System.currentTimeMillis() - lastAccess) < SYNC_STUCK_THRESHOLD_MILLIS;
    }

    boolean isCompletedOrStuck() {
        return mapsLeft.get() == 0 || !isActive();
    }

    private void access() {
        lastAccess = System.currentTimeMillis();
    }

    @Override
    public String toString() {
        return "WanSyncContext{"
                + "uuid=" + uuid
                + ", mapNames=" + mapNames
                + ", counterMaps=" + counterMaps
                + ", syncStatsMap=" + syncStatsMap
                + ", ownedPartitions=" + ownedPartitions
                + ", mapsLeft=" + mapsLeft
                + ", lastAccess=" + lastAccess
                + '}';
    }
}
