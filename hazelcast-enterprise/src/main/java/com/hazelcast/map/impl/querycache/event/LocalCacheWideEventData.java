package com.hazelcast.map.impl.querycache.event;

import com.hazelcast.map.impl.EventData;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * {@link EventData} which is used only for the subscriber end of a query cache
 * and only for query cache wide events like clearing all items together.
 * For this reason, it is not sent over the wire and is used local to query cache.
 * <p/>
 * Throws {@link UnsupportedOperationException} if one tries to serialize an instance  of this class.
 */
public class LocalCacheWideEventData implements EventData {

    private final String source;
    private final int eventType;
    private final int numberOfEntriesAffected;

    public LocalCacheWideEventData(String source, int eventType, int numberOfEntriesAffected) {
        this.source = source;
        this.eventType = eventType;
        this.numberOfEntriesAffected = numberOfEntriesAffected;
    }

    public int getNumberOfEntriesAffected() {
        return numberOfEntriesAffected;
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
        return "LocalCacheWideEventData{"
                + "eventType=" + eventType
                + ", source='" + source + '\''
                + ", numberOfEntriesAffected=" + numberOfEntriesAffected
                + '}';
    }
}
