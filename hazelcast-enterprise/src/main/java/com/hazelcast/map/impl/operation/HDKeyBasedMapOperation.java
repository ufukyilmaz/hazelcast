package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.NamedOperation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.util.Clock;

import java.io.IOException;

public abstract class HDKeyBasedMapOperation
        extends HDMapOperation
        implements PartitionAwareOperation, NamedOperation, Versioned {

    protected Data dataKey;
    protected long threadId;
    protected Data dataValue;
    protected long ttl = -1L;
    protected long maxIdle = -1L;


    public HDKeyBasedMapOperation() {
    }

    public HDKeyBasedMapOperation(String name, Data dataKey) {
        super(name);
        this.dataKey = dataKey;
    }

    protected HDKeyBasedMapOperation(String name, Data dataKey, Data dataValue) {
        super(name);
        this.dataKey = dataKey;
        this.dataValue = dataValue;
    }

    protected HDKeyBasedMapOperation(String name, Data dataKey, long ttl, long maxIdle) {
        super(name);
        this.dataKey = dataKey;
        this.ttl = ttl;
        this.maxIdle = maxIdle;
    }

    protected HDKeyBasedMapOperation(String name, Data dataKey, Data dataValue, long ttl, long maxIdle) {
        super(name);
        this.dataKey = dataKey;
        this.dataValue = dataValue;
        this.ttl = ttl;
        this.maxIdle = maxIdle;
    }

    protected long getNow() {
        return Clock.currentTimeMillis();
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public final Data getKey() {
        return dataKey;
    }

    @Override
    public final long getThreadId() {
        return threadId;
    }

    @Override
    public final void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    public final Data getValue() {
        return dataValue;
    }

    public final long getTtl() {
        return ttl;
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeData(dataKey);
        out.writeLong(threadId);
        out.writeData(dataValue);
        out.writeLong(ttl);
        //RU_COMPAT_3_10
        if (out.getVersion().isGreaterOrEqual(Versions.V3_11)) {
            out.writeLong(maxIdle);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        dataKey = in.readData();
        threadId = in.readLong();
        dataValue = in.readData();
        ttl = in.readLong();
        //RU_COMPAT_3_10
        if (in.getVersion().isGreaterOrEqual(Versions.V3_11)) {
            maxIdle = in.readLong();
        }
    }
}
