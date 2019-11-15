package com.hazelcast.cache.impl.wan;

import com.hazelcast.cache.CacheEntryView;
import com.hazelcast.enterprise.wan.impl.operation.EWRDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * WAN heap based implementation of {@link CacheEntryView}.
 */
public class WanCacheEntryView implements CacheEntryView<Data, Data>, IdentifiedDataSerializable {

    private Data key;
    private Data value;
    private long creationTime;
    private long expirationTime;
    private long lastAccessTime;
    private long hits;

    public WanCacheEntryView() {
    }

    WanCacheEntryView(CacheEntryView<Data, Data> entryView) {
        this.key = entryView.getKey();
        this.value = entryView.getValue();
        this.creationTime = entryView.getCreationTime();
        this.expirationTime = entryView.getExpirationTime();
        this.lastAccessTime = entryView.getLastAccessTime();
        this.hits = entryView.getHits();
    }

    public WanCacheEntryView(Data key, Data value, long creationTime,
                             long expirationTime, long lastAccessTime, long hits) {
        this.key = key;
        this.value = value;
        this.creationTime = creationTime;
        this.expirationTime = expirationTime;
        this.lastAccessTime = lastAccessTime;
        this.hits = hits;
    }

    @Override
    public Data getKey() {
        return key;
    }

    @Override
    public Data getValue() {
        return value;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public long getExpirationTime() {
        return expirationTime;
    }

    @Override
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    @Override
    public long getHits() {
        return hits;
    }

    @Override
    public Data getExpiryPolicy() {
        return null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(creationTime);
        out.writeLong(expirationTime);
        out.writeLong(lastAccessTime);
        out.writeLong(hits);
        out.writeData(key);
        out.writeData(value);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        creationTime = in.readLong();
        expirationTime = in.readLong();
        lastAccessTime = in.readLong();
        hits = in.readLong();
        key = in.readData();
        value = in.readData();
    }

    @Override
    public int getFactoryId() {
        return EWRDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return EWRDataSerializerHook.WAN_CACHE_ENTRY_VIEW;
    }
}