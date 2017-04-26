package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.impl.LockAwareLazyMapEntry;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.event.MapEventPublisher;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.impl.MutatingOperation;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.map.impl.EntryViews.createSimpleEntryView;

public class HDEntryBackupOperation extends HDKeyBasedMapOperation implements BackupOperation, MutatingOperation {

    protected transient Object oldValue;
    private EntryBackupProcessor entryProcessor;

    public HDEntryBackupOperation() {
    }

    public HDEntryBackupOperation(String name, Data dataKey, EntryBackupProcessor entryProcessor) {
        super(name, dataKey);
        this.entryProcessor = entryProcessor;
    }

    @Override
    public void innerBeforeRun() throws Exception {
        super.innerBeforeRun();

        if (entryProcessor instanceof HazelcastInstanceAware) {
            HazelcastInstance hazelcastInstance = getNodeEngine().getHazelcastInstance();
            ((HazelcastInstanceAware) entryProcessor).setHazelcastInstance(hazelcastInstance);
        }
    }

    @Override
    protected void runInternal() {
        oldValue = recordStore.get(dataKey, true);

        Map.Entry entry = createMapEntry(dataKey, oldValue);

        entryProcessor.processBackup(entry);

        if (noOp(entry, oldValue)) {
            return;
        }

        if (entryRemovedBackup(entry)) {
            return;
        }

        entryAddedOrUpdatedBackup(entry);
    }

    private void publishWanReplicationEvent(EntryEventType eventType) {
        final MapContainer mapContainer = this.mapContainer;
        if (!mapContainer.isWanReplicationEnabled()) {
            return;
        }
        final MapEventPublisher mapEventPublisher = mapContainer.getMapServiceContext().getMapEventPublisher();
        final Data key = dataKey;

        if (EntryEventType.REMOVED == eventType) {
            mapEventPublisher.publishWanReplicationRemoveBackup(name, key, Clock.currentTimeMillis());
        } else {
            final Record record = recordStore.getRecord(key);
            if (record != null) {
                dataValue = mapContainer.getMapServiceContext().toData(dataValue);
                final EntryView entryView = createSimpleEntryView(key, dataValue, record);
                mapEventPublisher.publishWanReplicationUpdateBackup(name, entryView);
            }
        }
    }

    @Override
    public void afterRun() throws Exception {
        evict(dataKey);
        disposeDeferredBlocks();
    }

    private boolean entryRemovedBackup(Map.Entry entry) {
        final Object value = entry.getValue();
        if (value == null) {
            recordStore.removeBackup(dataKey);
            publishWanReplicationEvent(EntryEventType.REMOVED);
            return true;
        }
        return false;
    }

    private void entryAddedOrUpdatedBackup(Map.Entry entry) {
        Object value = entry.getValue();
        recordStore.putBackup(dataKey, value);
        publishWanReplicationEvent(EntryEventType.UPDATED);
    }

    private Map.Entry createMapEntry(Data key, Object value) {
        InternalSerializationService serializationService
                = ((InternalSerializationService) getNodeEngine().getSerializationService());
        boolean locked = recordStore.isLocked(key);
        return new LockAwareLazyMapEntry(key, value, serializationService, mapContainer.getExtractors(), locked);
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

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.ENTRY_BACKUP;
    }
}
