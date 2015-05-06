package com.hazelcast.map.impl.querycache.event;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.util.Clock;

import java.io.IOException;

/**
 * Default implementation of {@link SingleEventData} which is sent to subscriber.
 */
public class DefaultSingleEventData implements SingleEventData {

    private Object key;
    private Object value;
    private Data dataKey;
    private Data dataNewValue;
    private Data dataOldValue;
    private long sequence;
    private SerializationService serializationService;
    private final long creationTime;
    private int eventType;
    private int partitionId;

    public DefaultSingleEventData() {
        creationTime = Clock.currentTimeMillis();
    }

    @Override
    public Object getKey() {
        if (key == null && dataKey != null) {
            key = serializationService.toObject(dataKey);
        }
        return key;
    }

    @Override
    public Object getValue() {
        if (value == null && dataNewValue != null) {
            value = serializationService.toObject(dataNewValue);
        }
        return value;
    }

    @Override
    public Data getDataKey() {
        return dataKey;
    }

    @Override
    public Data getDataNewValue() {
        return dataNewValue;
    }

    @Override
    public Data getDataOldValue() {
        return dataOldValue;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public long getSequence() {
        return sequence;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public int getEventType() {
        return eventType;
    }

    @Override
    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    public void setDataKey(Data dataKey) {
        this.dataKey = dataKey;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public void setDataNewValue(Data dataNewValue) {
        this.dataNewValue = dataNewValue;
    }

    public void setDataOldValue(Data dataOldValue) {
        this.dataOldValue = dataOldValue;
    }

    public void setEventType(int eventType) {
        this.eventType = eventType;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    @Override
    public void setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public String getSource() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getMapName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Address getCaller() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(sequence);
        out.writeData(dataKey);
        out.writeData(dataNewValue);
        out.writeInt(eventType);
        out.writeInt(partitionId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.sequence = in.readLong();
        this.dataKey = in.readData();
        this.dataNewValue = in.readData();
        this.eventType = in.readInt();
        this.partitionId = in.readInt();
    }

    @Override
    public String toString() {
        return "DefaultSingleEventData{"
                + "creationTime=" + creationTime
                + ", eventType=" + eventType
                + ", sequence=" + sequence
                + ", partitionId=" + partitionId
                + '}';
    }
}
