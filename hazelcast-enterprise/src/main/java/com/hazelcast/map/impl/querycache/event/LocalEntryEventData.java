package com.hazelcast.map.impl.querycache.event;

import com.hazelcast.map.impl.EventData;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

import java.io.IOException;

/**
 * {@link EventData} which is used only for the subscriber end of a query cache
 * and only for entry based events.
 * For this reason, it is not sent over the wire and is used local to query cache.
 * <p/>
 * Throws {@link UnsupportedOperationException} if one tries to serialize an instance  of this class.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 */
public class LocalEntryEventData<K, V> implements EventData {

    private K key;
    private V value;
    private V oldValue;
    private String source;
    private int eventType;
    private Data keyData;
    private Data valueData;
    private Data oldValueData;
    private final SerializationService serializationService;
    private final int partitionId;

    public LocalEntryEventData(SerializationService serializationService, String source,
                               int eventType, Object key, Object oldValue, Object value, int partitionId) {
        this.serializationService = serializationService;
        this.partitionId = partitionId;

        if (key instanceof Data) {
            this.keyData = (Data) key;
        } else {
            this.key = (K) key;
        }

        if (value instanceof Data) {
            this.valueData = (Data) value;
        } else {
            this.value = (V) value;
        }

        if (oldValue instanceof Data) {
            this.oldValueData = (Data) oldValue;
        } else {
            this.oldValue = (V) oldValue;
        }

        this.source = source;
        this.eventType = eventType;
    }

    public V getValue() {
        if (value == null && serializationService != null) {
            value = serializationService.toObject(valueData);
        }
        return value;
    }

    public V getOldValue() {
        if (oldValue == null && serializationService != null) {
            oldValue = serializationService.toObject(oldValueData);
        }
        return oldValue;
    }

    public K getKey() {
        if (key == null && serializationService != null) {
            key = serializationService.toObject(keyData);
        }
        return key;
    }

    public Data getKeyData() {
        return keyData;
    }

    public Data getValueData() {
        return valueData;
    }

    public Data getOldValueData() {
        return oldValueData;
    }

    @Override
    public String getSource() {
        return source;
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
    public int getEventType() {
        return eventType;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public LocalEntryEventData<K, V> cloneWithoutValue() {
        return new LocalEntryEventData<K, V>(serializationService, source, eventType, key, null, null, partitionId);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "LocalEntryEventData{"
                + "eventType=" + eventType
                + ", key=" + key
                + ", source='" + source + '\''
                + '}';
    }
}
