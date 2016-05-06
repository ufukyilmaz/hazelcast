package com.hazelcast.map.impl.querycache.subscriber.record;

import com.hazelcast.util.Clock;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Contains common functionality which is needed by a {@link QueryCacheRecord} instance.
 */
abstract class AbstractQueryCacheRecord implements QueryCacheRecord {

    protected final long creationTime;
    protected volatile long accessTime = -1L;
    protected volatile int accessHit;

    public AbstractQueryCacheRecord() {
        creationTime = Clock.currentTimeMillis();
    }

    @Override
    public int getAccessHit() {
        return accessHit;
    }

    @Override
    public long getLastAccessTime() {
        return accessTime;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT",
            justification = "CacheRecord can be accessed by only its own partition thread.")
    public void incrementAccessHit() {
        accessHit++;
    }

    @Override
    public void setAccessTime(long accessTime) {
        this.accessTime = accessTime;
    }

}
