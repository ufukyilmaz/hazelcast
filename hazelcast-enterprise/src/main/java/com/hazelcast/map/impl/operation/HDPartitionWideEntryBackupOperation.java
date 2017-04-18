package com.hazelcast.map.impl.operation;

import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class HDPartitionWideEntryBackupOperation extends AbstractHDMultipleEntryOperation implements BackupOperation {

    public HDPartitionWideEntryBackupOperation() {
    }

    public HDPartitionWideEntryBackupOperation(String name, EntryBackupProcessor backupProcessor) {
        super(name, backupProcessor);
    }

    @Override
    protected void runInternal() {
        final long now = getNow();
        final int entryCount = recordStore.size();

        Container removedEntryRecordPairs = new Container(entryCount, newEntryRemoveHandler(now, true));
        Container addedOrUpdatedEntryRecordPairs = new Container(entryCount, newEntryAddOrUpdateHandler(now, true));
        responses = new MapEntries(entryCount);

        Iterator<Record> iterator = recordStore.iterator(now, false);
        while (iterator.hasNext()) {
            Record record = iterator.next();
            Data dataKey = record.getKey();
            Object oldValue = record.getValue();

            if (!applyPredicate(dataKey, oldValue)) {
                continue;
            }

            Map.Entry entry = createMapEntry(dataKey, oldValue);
            processBackup(entry);

            // First call noOp, other if checks below depends on it.
            if (noOp(entry, oldValue)) {
                continue;
            }

            if (isEntryRemoved(entry)) {
                removedEntryRecordPairs.add(entry, record);
                continue;
            }

            addedOrUpdatedEntryRecordPairs.add(entry, record);
        }

        removedEntryRecordPairs.process();
        addedOrUpdatedEntryRecordPairs.process();
    }

    @Override
    public void afterRun() throws Exception {
        evict(null);
        super.afterRun();
    }


    protected Predicate getPredicate() {
        return null;
    }

    @Override
    public Object getResponse() {
        return true;
    }

    private boolean applyPredicate(Data key, Object value) {
        if (getPredicate() == null) {
            return true;
        }
        QueryableEntry queryEntry = mapContainer.newQueryEntry(key, value);
        return getPredicate().apply(queryEntry);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        backupProcessor = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(backupProcessor);
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.PARTITION_WIDE_ENTRY_BACKUP;
    }
}
