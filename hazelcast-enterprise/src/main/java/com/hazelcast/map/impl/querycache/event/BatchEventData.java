package com.hazelcast.map.impl.querycache.event;

import com.hazelcast.map.impl.EventData;
import com.hazelcast.map.impl.querycache.event.sequence.Sequenced;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.map.impl.querycache.event.QueryCacheEventDataBuilder.newQueryCacheEventDataBuilder;
import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Holder for a collection of {@link QueryCacheEventData}
 *
 * @see QueryCacheEventData
 */
public class BatchEventData implements Sequenced, EventData {

    private String source;
    private List<QueryCacheEventData> events;
    private transient int partitionId;

    public BatchEventData() {
    }

    public BatchEventData(List<QueryCacheEventData> events, String source, int partitionId) {
        this.events = checkNotNull(events, "events cannot be null");
        this.source = checkNotNull(source, "source cannot be null");
        this.partitionId = checkNotNegative(partitionId, "partitionId cannot be negative");
    }

    public void add(QueryCacheEventData entry) {
        events.add(entry);
    }

    public List<QueryCacheEventData> getEvents() {
        return events;
    }

    public boolean isEmpty() {
        return events.isEmpty();
    }

    public int size() {
        return events.size();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        Collection<QueryCacheEventData> events = this.events;
        out.writeUTF(source);
        out.writeInt(events.size());
        for (QueryCacheEventData eventData : events) {
            eventData.writeData(out);
        }
    }


    @Override
    public void readData(ObjectDataInput in) throws IOException {
        source = in.readUTF();
        int size = in.readInt();
        if (size > 0) {
            this.events = new ArrayList<QueryCacheEventData>(size);
        }
        Collection<QueryCacheEventData> events = this.events;
        for (int i = 0; i < size; i++) {
            QueryCacheEventData eventData = newQueryCacheEventDataBuilder().build();
            eventData.readData(in);

            events.add(eventData);
        }
    }

    @Override
    public String toString() {
        return "BatchEventData{}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BatchEventData)) {
            return false;
        }

        BatchEventData that = (BatchEventData) o;

        if (events != null ? !events.equals(that.events) : that.events != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return events != null ? events.hashCode() : 0;
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
        throw new UnsupportedOperationException();
    }

    @Override
    public long getSequence() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public void setSequence(long sequence) {
        throw new UnsupportedOperationException();
    }
}

