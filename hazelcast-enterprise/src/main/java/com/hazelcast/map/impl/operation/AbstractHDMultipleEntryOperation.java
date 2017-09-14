package com.hazelcast.map.impl.operation;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.IBiFunction;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.LazyMapEntry;
import com.hazelcast.map.impl.LocalMapStatsProvider;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.util.ToHeapDataConverter.toHeapData;
import static com.hazelcast.map.impl.EntryViews.createSimpleEntryView;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.recordstore.RecordStore.DEFAULT_TTL;

abstract class AbstractHDMultipleEntryOperation extends HDMapOperation implements MutatingOperation, PartitionAwareOperation {

    protected MapEntries responses;
    protected EntryProcessor entryProcessor;
    protected EntryBackupProcessor backupProcessor;
    protected List<WanEventWrapper> wanEventList = new ArrayList<WanEventWrapper>();

    protected AbstractHDMultipleEntryOperation() {
    }

    protected AbstractHDMultipleEntryOperation(String name, EntryProcessor entryProcessor) {
        super(name);
        this.entryProcessor = entryProcessor;
    }

    protected AbstractHDMultipleEntryOperation(String name, EntryBackupProcessor backupProcessor) {
        super(name);
        this.backupProcessor = backupProcessor;
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();

        disposeDeferredBlocks();
    }

    protected Map.Entry createMapEntry(Data key, Object value) {
        InternalSerializationService serializationService
                = (InternalSerializationService) getNodeEngine().getSerializationService();
        return new LazyMapEntry(key, value, serializationService, mapContainer.getExtractors());
    }

    protected boolean hasRegisteredListenerForThisMap() {
        final EventService eventService = getNodeEngine().getEventService();
        return eventService.hasEventRegistration(SERVICE_NAME, name);
    }

    /**
     * Nullify old value if in-memory format is object and operation is not removal
     * since old and new value in fired event {@link com.hazelcast.core.EntryEvent}
     * may be same due to the object in-memory format.
     */
    protected Object nullifyOldValueIfNecessary(Object oldValue, EntryEventType eventType) {
        final MapConfig mapConfig = mapContainer.getMapConfig();
        final InMemoryFormat format = mapConfig.getInMemoryFormat();
        if (format == InMemoryFormat.OBJECT && eventType != EntryEventType.REMOVED) {
            return null;
        } else {
            return oldValue;
        }
    }

    protected LocalMapStatsImpl getLocalMapStats() {
        final LocalMapStatsProvider localMapStatsProvider = mapServiceContext.getLocalMapStatsProvider();
        return localMapStatsProvider.getLocalMapStatsImpl(name);
    }

    private EntryEventType pickEventTypeOrNull(Map.Entry entry, Object oldValue) {
        final Object value = entry.getValue();
        if (value == null) {
            return EntryEventType.REMOVED;
        } else {
            if (oldValue == null) {
                return EntryEventType.ADDED;
            }
            final LazyMapEntry mapEntrySimple = (LazyMapEntry) entry;
            if (mapEntrySimple.isModified()) {
                return EntryEventType.UPDATED;
            }
        }
        // return null for read only operations
        return null;
    }

    /**
     * Entry has not been existing and no add operation has been done.
     */
    protected boolean noOp(Map.Entry entry, Object oldValue) {
        final LazyMapEntry mapEntrySimple = (LazyMapEntry) entry;
        return !mapEntrySimple.isModified() || (oldValue == null && entry.getValue() == null);
    }

    protected static boolean isEntryRemoved(Map.Entry entry) {
        return entry.getValue() == null;
    }

    protected boolean entryRemoved(Map.Entry entry, Data key, Object oldValue, long now) {
        final Object value = entry.getValue();
        if (value == null) {
            recordStore.delete(key);
            getLocalMapStats().incrementRemoves(getLatencyFrom(now));
            doPostOps(key, oldValue, entry);
            return true;
        }
        return false;
    }

    protected boolean entryAddedOrUpdated(Map.Entry entry, Data key, Object oldValue, long now) {
        final Object value = entry.getValue();
        if (value != null) {
            recordStore.set(key, value, DEFAULT_TTL);
            getLocalMapStats().incrementPuts(getLatencyFrom(now));
            doPostOps(key, oldValue, entry);
            return true;
        }
        return false;
    }

    protected void doPostOps(Data key, Object oldValue, Map.Entry entry) {
        final EntryEventType eventType = pickEventTypeOrNull(entry, oldValue);
        if (eventType == null) {
            return;
        }

        key = toHeapData(key);
        Object newValue = entry.getValue();
        invalidateNearCache(key);
        mapServiceContext.interceptAfterPut(name, newValue);
        if (isPostProcessing(recordStore)) {
            Record record = recordStore.getRecord(key);
            newValue = record == null ? null : record.getValue();
        }
        if (mapContainer.isWanReplicationEnabled()) {
            newValue = toData(newValue);
            publishWanReplicationEvent(key, (Data) newValue, eventType);
        }
        publishEntryEvent(key, newValue, oldValue, eventType);
    }

    protected boolean entryRemovedBackup(Map.Entry entry, Data key) {
        final Object value = entry.getValue();
        if (value == null) {
            recordStore.removeBackup(key);
            return true;
        }
        return false;
    }

    protected boolean entryAddedOrUpdatedBackup(Map.Entry entry, Data key) {
        final Object value = entry.getValue();
        if (value != null) {
            recordStore.putBackup(key, value);
            return true;
        }
        return false;
    }

    protected Data toData(Object obj) {
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.toData(obj);
    }

    protected long getNow() {
        return Clock.currentTimeMillis();
    }

    protected void publishEntryEvent(Data key, Object value, Object oldValue, EntryEventType eventType) {
        if (hasRegisteredListenerForThisMap()) {
            oldValue = nullifyOldValueIfNecessary(oldValue, eventType);
            mapServiceContext.getMapEventPublisher()
                    .publishEvent(getCallerAddress(), name, eventType, key, oldValue, value);
        }
    }

    protected void publishWanReplicationEvent(Data key, Data value, EntryEventType eventType) {
        if (EntryEventType.REMOVED == eventType) {
            mapEventPublisher.publishWanReplicationRemove(name, key, getNow());
            wanEventList.add(new WanEventWrapper(key, null, EntryEventType.REMOVED));
        } else {
            final Record record = recordStore.getRecord(key);
            if (record != null) {
                final Data dataValueAsData = toData(value);
                final EntryView entryView = createSimpleEntryView(key, dataValueAsData, record);
                mapEventPublisher.publishWanReplicationUpdate(name, entryView);
                wanEventList.add(new WanEventWrapper(key, value, EntryEventType.UPDATED));
            }
        }
    }

    protected long getLatencyFrom(long begin) {
        return Clock.currentTimeMillis() - begin;
    }

    protected Data process(Map.Entry entry) {
        final Object result = entryProcessor.process(entry);
        return toData(result);
    }

    protected void processBackup(Map.Entry entry) {
        backupProcessor.processBackup(entry);
    }

    protected boolean isKeyProcessable(Data key) {
        final IPartitionService partitionService = getNodeEngine().getPartitionService();
        return partitionService.getPartitionId(key) != getPartitionId();
    }

    // this method is overridden
    protected boolean isEntryProcessable(Map.Entry entry) {
        return true;
    }

    public void setWanEventList(List<WanEventWrapper> wanEventList) {
        this.wanEventList = wanEventList;
    }

    protected class WanEventWrapper {

        Data key;
        Data value;
        EntryEventType eventType;

        public WanEventWrapper(Data key, Data value, EntryEventType eventType) {
            this.key = toData(key);
            this.value = toData(value);
            this.eventType = eventType;
        }

        public Data getKey() {
            return key;
        }

        public void setKey(Data key) {
            this.key = key;
        }

        public Data getValue() {
            return value;
        }

        public void setValue(Data value) {
            this.value = value;
        }

        public EntryEventType getEventType() {
            return eventType;
        }

        public void setEventType(EntryEventType eventType) {
            this.eventType = eventType;
        }
    }

    protected IBiFunction<Map.Entry, Record, Void> newEntryAddOrUpdateHandler(final long now, final boolean backup) {
        return new IBiFunction<Map.Entry, Record, Void>() {
            @Override
            public Void apply(Map.Entry entry, Record record) {
                if (backup) {
                    entryAddedOrUpdatedBackup(entry, record.getKey());
                } else {
                    entryAddedOrUpdated(entry, record.getKey(), record.getValue(), now);
                }
                return null;
            }
        };
    }

    protected IBiFunction<Map.Entry, Record, Void> newEntryRemoveHandler(final long now, final boolean backup) {
        return new IBiFunction<Map.Entry, Record, Void>() {
            @Override
            public Void apply(Map.Entry entry, Record record) {
                if (backup) {
                    entryRemovedBackup(entry, record.getKey());
                } else {
                    entryRemoved(entry, record.getKey(), record.getValue(), now);
                }
                return null;
            }
        };
    }

    static final class Container {
        int size;
        List<Object> objects;
        IBiFunction<Map.Entry, Record, Void> handler;

        Container(int size, IBiFunction<Map.Entry, Record, Void> handler) {
            this.size = size;
            this.handler = handler;
        }

        void add(Map.Entry entry, Record record) {
            if (objects == null) {
                objects = new ArrayList<Object>(size);
            }

            objects.add(entry);
            objects.add(record);
        }

        void process() {
            if (objects == null) {
                return;
            }

            for (int i = 0; i < objects.size(); ) {
                Map.Entry entry = (Map.Entry) objects.get(i++);
                Record record = (Record) objects.get(i++);

                handler.apply(entry, record);
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);

        out.writeInt(wanEventList.size());
        for (WanEventWrapper wanEventWrapper : wanEventList) {
            out.writeData(wanEventWrapper.getKey());
            out.writeData(wanEventWrapper.getValue());
            out.writeInt(wanEventWrapper.getEventType().getType());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            Data key = in.readData();
            Data value = in.readData();
            EntryEventType entryEventType = EntryEventType.getByType(in.readInt());
            wanEventList.add(new WanEventWrapper(key, value, entryEventType));
        }
    }
}
