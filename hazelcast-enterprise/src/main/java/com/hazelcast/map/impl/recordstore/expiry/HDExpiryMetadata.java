package com.hazelcast.map.impl.recordstore.expiry;

import com.hazelcast.internal.hidensity.HiDensityRecord;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.map.impl.record.Record.UNSET;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class HDExpiryMetadata extends HiDensityRecord
        implements ExpiryMetadata {

    public static final int SIZE;

    private static final int TTL_OFFSET = 0;
    private static final int MAX_IDLE_OFFSET = TTL_OFFSET + INT_SIZE_IN_BYTES;
    private static final int EXPIRATION_TIME_OFFSET = MAX_IDLE_OFFSET + INT_SIZE_IN_BYTES;

    static {
        SIZE = EXPIRATION_TIME_OFFSET + INT_SIZE_IN_BYTES;
    }

    public HDExpiryMetadata() {
        super(AMEM);
    }

    public HDExpiryMetadata(long address) {
        super(AMEM, address, SIZE);
    }

    @Override
    public long getValueAddress() {
        return address;
    }

    @Override
    public void setValueAddress(long valueAddress) {
        writeLong(address, valueAddress);
    }

    public HDExpiryMetadata reset(long address) {
        setAddress(address);
        setSize(SIZE);
        return this;
    }

    @Override
    public void clear() {
        zero();
    }

    @Override
    public long getSequence() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getTtl() {
        int ttl = readInt(TTL_OFFSET);
        return ttl == Integer.MAX_VALUE ? Long.MAX_VALUE : SECONDS.toMillis(ttl);
    }

    @Override
    public int getRawTtl() {
        return readInt(TTL_OFFSET);
    }

    @Override
    public ExpiryMetadata setTtl(long ttl) {
        long ttlSeconds = MILLISECONDS.toSeconds(ttl);
        if (ttlSeconds == 0 && ttl != 0) {
            ttlSeconds = 1;
        }

        writeInt(TTL_OFFSET, ttlSeconds > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) ttlSeconds);
        return this;
    }

    @Override
    public ExpiryMetadata setRawTtl(int ttl) {
        writeInt(TTL_OFFSET, ttl);
        return this;
    }

    @Override
    public long getMaxIdle() {
        int maxIdle = readInt(MAX_IDLE_OFFSET);
        return maxIdle == Integer.MAX_VALUE ? Long.MAX_VALUE : SECONDS.toMillis(maxIdle);
    }

    @Override
    public int getRawMaxIdle() {
        return readInt(MAX_IDLE_OFFSET);
    }

    @Override
    public ExpiryMetadata setMaxIdle(long maxIdle) {
        long maxIdleSeconds = MILLISECONDS.toSeconds(maxIdle);
        if (maxIdleSeconds == 0 && maxIdle != 0) {
            maxIdleSeconds = 1;
        }
        writeInt(MAX_IDLE_OFFSET, maxIdleSeconds > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) maxIdleSeconds);
        return this;
    }

    @Override
    public ExpiryMetadata setRawMaxIdle(int maxIdle) {
        writeInt(MAX_IDLE_OFFSET, maxIdle);
        return this;
    }

    @Override
    public long getExpirationTime() {
        int expirationTime = readInt(EXPIRATION_TIME_OFFSET);
        if (expirationTime == UNSET) {
            return 0L;
        }

        if (expirationTime == Integer.MAX_VALUE) {
            return Long.MAX_VALUE;
        }

        return recomputeWithBaseTime(expirationTime);
    }

    @Override
    public int getRawExpirationTime() {
        return readInt(EXPIRATION_TIME_OFFSET);
    }

    @Override
    public ExpiryMetadata setExpirationTime(long expirationTime) {
        writeInt(EXPIRATION_TIME_OFFSET, expirationTime == Long.MAX_VALUE
                ? Integer.MAX_VALUE
                : stripBaseTime(expirationTime));
        return this;
    }

    @Override
    public ExpiryMetadata setRawExpirationTime(int expirationTime) {
        writeInt(EXPIRATION_TIME_OFFSET, expirationTime);
        return this;
    }

    @Override
    public String toString() {
        return "HDExpiryMetadata{"
                + " ttl= " + getTtl()
                + ", idle=" + getMaxIdle()
                + ", expirationTime=" + getExpirationTime()
                + "}";
    }
}
