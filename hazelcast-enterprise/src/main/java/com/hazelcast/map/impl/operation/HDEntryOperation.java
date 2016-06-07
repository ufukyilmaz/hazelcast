package com.hazelcast.map.impl.operation;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.LazyMapEntry;
import com.hazelcast.map.impl.LocalMapStatsProvider;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.REMOVED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static com.hazelcast.map.impl.EntryViews.createSimpleEntryView;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.map.impl.recordstore.RecordStore.DEFAULT_TTL;

/**
 * GOTCHA : This operation LOADS missing keys from map-store, in contrast with PartitionWideEntryOperation.
 */
public class HDEntryOperation extends HDLockAwareOperation implements BackupAwareOperation, MutatingOperation {

    protected Object oldValue;
    private EntryProcessor entryProcessor;
    private EntryEventType eventType;
    private Object response;
    private transient Object dataValue;

    public HDEntryOperation() {
    }

    public HDEntryOperation(String name, Data dataKey, EntryProcessor entryProcessor) {
        super(name, dataKey);
        this.entryProcessor = entryProcessor;
    }

    @Override
    public void innerBeforeRun() throws Exception {
        super.innerBeforeRun();

        SerializationService serializationService = getNodeEngine().getSerializationService();
        ManagedContext managedContext = serializationService.getManagedContext();
        managedContext.initialize(entryProcessor);
    }

    @Override
    protected void runInternal() {
        final long now = getNow();
        oldValue = recordStore.get(dataKey, false);

        Map.Entry entry = createMapEntry(dataKey, oldValue);

        response = process(entry);

        // first call noOp, other if checks below depends on it.
        if (noOp(entry)) {
            return;
        }
        if (entryRemoved(entry, now)) {
            return;
        }
        entryAddedOrUpdated(entry, now);
    }

    @Override
    public void afterRun() throws Exception {
        super.afterRun();
        if (eventType == null) {
            return;
        }
        mapServiceContext.interceptAfterPut(name, dataValue);
        if (isPostProcessing(recordStore)) {
            Record record = recordStore.getRecord(dataKey);
            dataValue = record.getValue();
        }


        invalidateNearCache(dataKey);
        publishEntryEvent();
        publishWanReplicationEvent();
        evict(dataKey);

        disposeDeferredBlocks();
    }

    @Override
    public void onWaitExpire() {
        sendResponse(null);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public Operation getBackupOperation() {
        EntryBackupProcessor backupProcessor = entryProcessor.getBackupProcessor();
        return backupProcessor != null ? new HDEntryBackupOperation(name, dataKey, backupProcessor) : null;
    }

    @Override
    public boolean shouldBackup() {
        return mapContainer.getTotalBackupCount() > 0 && entryProcessor.getBackupProcessor() != null;
    }

    @Override
    public int getAsyncBackupCount() {
        return mapContainer.getAsyncBackupCount();
    }

    @Override
    public int getSyncBackupCount() {
        return mapContainer.getBackupCount();
    }

    private long getLatencyFrom(long begin) {
        return Clock.currentTimeMillis() - begin;
    }

    private Data toData(Object obj) {
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.toData(obj);
    }

    private long getNow() {
        return Clock.currentTimeMillis();
    }

    /**
     * noOp in two cases:
     * - setValue not called on entry
     * - or entry does not exist and no add operation is done.
     */
    private boolean noOp(Map.Entry entry) {
        final LazyMapEntry mapEntrySimple = (LazyMapEntry) entry;
        return !mapEntrySimple.isModified() || (oldValue == null && entry.getValue() == null);
    }

    private boolean entryRemoved(Map.Entry entry, long now) {
        final Object value = entry.getValue();
        if (value == null) {
            recordStore.delete(dataKey);
            getLocalMapStats().incrementRemoves(getLatencyFrom(now));
            eventType = REMOVED;
            return true;
        }
        return false;
    }

    /**
     * Only difference between add and update is event type to be published.
     */
    private void entryAddedOrUpdated(Map.Entry entry, long now) {
        dataValue = entry.getValue();
        recordStore.set(dataKey, dataValue, DEFAULT_TTL);
        getLocalMapStats().incrementPuts(getLatencyFrom(now));

        eventType = oldValue == null ? ADDED : UPDATED;
    }

    private Data process(Map.Entry entry) {
        final Object result = entryProcessor.process(entry);
        return toData(result);
    }

    private Map.Entry createMapEntry(Data key, Object value) {
        return new LazyMapEntry(key, value, (InternalSerializationService) getNodeEngine().getSerializationService());
    }

    private LocalMapStatsImpl getLocalMapStats() {
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        final LocalMapStatsProvider localMapStatsProvider = mapServiceContext.getLocalMapStatsProvider();
        return localMapStatsProvider.getLocalMapStatsImpl(name);
    }

    private boolean hasRegisteredListenerForThisMap() {
        final EventService eventService = getNodeEngine().getEventService();
        return eventService.hasEventRegistration(SERVICE_NAME, name);
    }

    /**
     * Nullify old value if in memory format is object and operation is not removal
     * since old and new value in fired event {@link com.hazelcast.core.EntryEvent}
     * may be same due to the object in memory format.
     */
    private void nullifyOldValueIfNecessary() {
        final MapConfig mapConfig = mapContainer.getMapConfig();
        final InMemoryFormat format = mapConfig.getInMemoryFormat();
        if (format == InMemoryFormat.OBJECT && eventType != REMOVED) {
            oldValue = null;
        }
    }

    private void publishEntryEvent() {
        if (hasRegisteredListenerForThisMap()) {
            nullifyOldValueIfNecessary();
            final MapEventPublisher mapEventPublisher = getMapEventPublisher();
            mapEventPublisher.publishEvent(getCallerAddress(), name, eventType, dataKey, oldValue, dataValue);
        }
    }

    private void publishWanReplicationEvent() {
        final MapContainer mapContainer = this.mapContainer;
        if (mapContainer.getWanReplicationPublisher() == null
                && mapContainer.getWanMergePolicy() == null) {
            return;
        }
        final MapEventPublisher mapEventPublisher = getMapEventPublisher();
        final Data key = dataKey;

        if (REMOVED.equals(eventType)) {
            mapEventPublisher.publishWanReplicationRemove(name, key, getNow());
        } else {
            final Record record = recordStore.getRecord(key);
            if (record != null) {
                dataValue = toData(dataValue);
                final EntryView entryView = createSimpleEntryView(key, dataValue, record);
                mapEventPublisher.publishWanReplicationUpdate(name, entryView);
            }
        }
    }

    private MapEventPublisher getMapEventPublisher() {
        final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getMapEventPublisher();
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        entryProcessor = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(entryProcessor);
    }
}
