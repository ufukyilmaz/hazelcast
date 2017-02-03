package com.hazelcast.map.impl.operation;

import com.hazelcast.core.ManagedContext;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.query.impl.FalsePredicate;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.serialization.SerializationService;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * GOTCHA : This operation does NOT load missing keys from map-store for now.
 */
public class HDPartitionWideEntryOperation extends AbstractHDMultipleEntryOperation implements BackupAwareOperation {

    public HDPartitionWideEntryOperation(String name, EntryProcessor entryProcessor) {
        super(name, entryProcessor);
    }

    public HDPartitionWideEntryOperation() {
    }

    @Override
    public void innerBeforeRun() throws Exception {
        super.innerBeforeRun();
        final SerializationService serializationService = getNodeEngine().getSerializationService();
        final ManagedContext managedContext = serializationService.getManagedContext();
        managedContext.initialize(entryProcessor);
    }

    @Override
    protected void runInternal() {
        long now = getNow();

        responses = new MapEntries(recordStore.size());
        Iterator<Record> iterator = recordStore.iterator(now, false);
        while (iterator.hasNext()) {
            Record record = iterator.next();
            Data dataKey = record.getKey();
            Object oldValue = record.getValue();

            if (!applyPredicate(dataKey, oldValue)) {
                continue;
            }

            Map.Entry entry = createMapEntry(dataKey, oldValue);
            Data response = process(entry);
            if (response != null) {
                // Copy key from Hi-Density memory to heap memory.
                responses.add(toData(dataKey), response);
            }

            // First call noOp, other if checks below depends on it.
            if (noOp(entry, oldValue)) {
                continue;
            }
            if (entryRemoved(entry, dataKey, oldValue, now)) {
                continue;
            }
            entryAddedOrUpdated(entry, dataKey, oldValue, now);

            evict(dataKey);
        }
    }

    @Override
    public Object getResponse() {
        return responses;
    }

    @Override
    public boolean shouldBackup() {
        return mapContainer.getTotalBackupCount() > 0 && entryProcessor.getBackupProcessor() != null;
    }

    @Override
    public int getSyncBackupCount() {
        return 0;
    }

    @Override
    public int getAsyncBackupCount() {
        return mapContainer.getTotalBackupCount();
    }

    @Override
    public Operation getBackupOperation() {
        EntryBackupProcessor backupProcessor = entryProcessor.getBackupProcessor();
        return backupProcessor != null ? new HDPartitionWideEntryBackupOperation(name, backupProcessor) : null;
    }

    private boolean applyPredicate(Data key, Object value) {
        Predicate predicate = getPredicate();

        if (predicate == null || TruePredicate.INSTANCE == predicate) {
            return true;
        }

        if (FalsePredicate.INSTANCE == predicate) {
            return false;
        }

        QueryableEntry queryEntry = mapContainer.newQueryEntry(key, value);
        return getPredicate().apply(queryEntry);
    }

    protected Predicate getPredicate() {
        return null;
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
        return EnterpriseMapDataSerializerHook.PARTITION_WIDE_ENTRY;
    }

}
