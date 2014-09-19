package com.hazelcast.cache;

import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author enesakar 2/19/14
 */
public class CacheStatsImpl implements DataSerializable, CacheStats {

    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();

    private final AtomicLong puts = new AtomicLong();
    private final AtomicLong asyncPuts = new AtomicLong();
    private final AtomicLong gets = new AtomicLong();
    private final AtomicLong asyncGets = new AtomicLong();
    private final AtomicLong removes = new AtomicLong();
    private final AtomicLong asyncRemoves = new AtomicLong();
    private final AtomicLong others = new AtomicLong();

    private volatile long creationTime;
    private volatile long lastUpdateTime;
    private volatile long lastAccessTime;
    private volatile long ownedEntryCount;

    private final AtomicLong totalPutLatency = new AtomicLong();
    private final AtomicLong totalGetLatency = new AtomicLong();
    private final AtomicLong totalRemoveLatency = new AtomicLong();
    private volatile NearCacheStats nearCacheStats;

    public CacheStatsImpl() {
    }

    public CacheStatsImpl(long creationTime) {
        this.creationTime = creationTime;
    }

    @Override
    public long getOwnedEntryCount() {
        return ownedEntryCount;
    }

    @Override
    public long getHits() {
        return hits.get();
    }

    @Override
    public long getMisses() {
        return misses.get();
    }

    @Override
    public long getPuts() {
        return puts.get() + asyncPuts.get();
    }

    @Override
    public long getGets() {
        return gets.get() + asyncGets.get();
    }

    @Override
    public long getRemoves() {
        return removes.get() + asyncRemoves.get();
    }

    @Override
    public double getAveragePutLatency() {
        return puts.get() <= 0 ? 0 : (double)totalPutLatency.get() / puts.get();
    }

    @Override
    public double getAverageGetLatency() {
        return gets.get() <= 0 ? 0 : (double)totalGetLatency.get() / gets.get();
    }

    @Override
    public double getAverageRemoveLatency() {
        return removes.get() <= 0 ? 0 : (double)totalRemoveLatency.get() / removes.get();
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setOwnedEntryCount(long ownedEntryCount) {
        this.ownedEntryCount = ownedEntryCount;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    @Override
    public NearCacheStats getNearCacheStats() {
        return nearCacheStats;
    }

    public void setNearCacheStats(NearCacheStats nearCacheStats) {
        this.nearCacheStats = nearCacheStats;
    }

    public void updatePutStats(long startTime) {
        puts.incrementAndGet();
        long currentTime = System.currentTimeMillis();
        lastUpdateTime = currentTime;
        totalPutLatency.addAndGet(currentTime - startTime);
    }

    public void updateRemoveStats(long startTime) {
        removes.incrementAndGet();
        long currentTime = System.currentTimeMillis();
        lastUpdateTime = currentTime;
        totalRemoveLatency.addAndGet(currentTime - startTime);
    }

    public void updateGetStats(boolean hit, long startTime) {
        gets.incrementAndGet();
        long currentTime = System.currentTimeMillis();
        lastAccessTime = currentTime;
        if (hit) {
            hits.incrementAndGet();
        } else {
            misses.incrementAndGet();
        }
        totalGetLatency.addAndGet(currentTime - startTime);
    }

    public void updateGetStats(long startTime) {
        gets.incrementAndGet();
        long currentTime = System.currentTimeMillis();
        lastAccessTime = currentTime;
        totalGetLatency.addAndGet(currentTime - startTime);
    }

    public void updateGetStats(boolean hit) {
        gets.incrementAndGet();
        lastAccessTime = System.currentTimeMillis();
        if (hit) {
            hits.incrementAndGet();
        } else {
            misses.incrementAndGet();
        }
    }

    public void updateGetStats() {
        gets.incrementAndGet();
        lastAccessTime = System.currentTimeMillis();
    }

    public void updateAsyncGetStats() {
        asyncGets.incrementAndGet();
        lastAccessTime = System.currentTimeMillis();
    }

    public void updatePutStats() {
        puts.incrementAndGet();
        lastUpdateTime = System.currentTimeMillis();
    }
    public void updateAsyncPutStats() {
        asyncPuts.incrementAndGet();
        lastUpdateTime = System.currentTimeMillis();
    }

    public void updateRemoveStats() {
        removes.incrementAndGet();
        lastUpdateTime = System.currentTimeMillis();
    }

    public void updateAsyncRemoveStats() {
        asyncRemoves.incrementAndGet();
        lastUpdateTime = System.currentTimeMillis();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(ownedEntryCount);
        out.writeLong(misses.get());
        out.writeLong(hits.get());
        out.writeLong(puts.get());
        out.writeLong(asyncPuts.get());
        out.writeLong(gets.get());
        out.writeLong(asyncGets.get());
        out.writeLong(removes.get());
        out.writeLong(asyncRemoves.get());
        out.writeLong(others.get());
        out.writeLong(totalGetLatency.get());
        out.writeLong(totalPutLatency.get());
        out.writeLong(totalRemoveLatency.get());
        out.writeLong(creationTime);
        out.writeLong(lastAccessTime);
        out.writeLong(lastUpdateTime);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        ownedEntryCount = in.readLong();
        misses.set(in.readLong());
        hits.set(in.readLong());
        puts.set(in.readLong());
        asyncPuts.set(in.readLong());
        gets.set(in.readLong());
        asyncGets.set(in.readLong());
        removes.set(in.readLong());
        asyncRemoves.set(in.readLong());
        others.set(in.readLong());
        totalGetLatency.set(in.readLong());
        totalPutLatency.set(in.readLong());
        totalRemoveLatency.set(in.readLong());
        creationTime = in.readLong();
        lastAccessTime = in.readLong();
        lastUpdateTime = in.readLong();
    }
}
