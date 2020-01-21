package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.QueryOptimizer;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import static com.hazelcast.internal.util.ToHeapDataConverter.toHeapData;
import static com.hazelcast.map.impl.operation.EntryOperator.operator;

/**
 * GOTCHA: This operation does NOT load missing keys from the MapStore for now.
 */
public class HDPartitionWideEntryOperation extends AbstractHDMultipleEntryOperation implements BackupAwareOperation {

    protected transient EntryOperator operator;
    protected transient QueryOptimizer queryOptimizer;
    protected transient Set<Data> keysFromIndex;

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
        keysFromIndex = null;
        queryOptimizer = mapServiceContext.getQueryOptimizer();
    }

    @Override
    protected void runInternal() {
        if (runWithIndex()) {
            return;
        }
        runWithPartitionScan();
    }

    /**
     * @return {@code true} if index has been used and the EP has been executed on its keys, {@code false} otherwise
     */
    private boolean runWithIndex() {
        // here we try to query the partitioned-index
        if (getPredicate() != null) {
            // we use the partitioned-index to operate on the selected keys only
            Indexes indexes = mapContainer.getIndexes(getPartitionId());
            Set<QueryableEntry> entries = indexes.query(queryOptimizer.optimize(getPredicate(), indexes), 1);
            if (entries != null) {
                responses = new MapEntries(entries.size());

                // we can pass null as predicate since it's all happening on partition thread so no data-changes may occur
                operator = operator(this, entryProcessor, null);
                keysFromIndex = new HashSet<Data>(entries.size());
                for (QueryableEntry entry : entries) {
                    keysFromIndex.add(entry.getKeyData());
                    Data response = operator.operateOnKey(entry.getKeyData()).doPostOperateOps().getResult();
                    if (response != null) {
                        responses.add(entry.getKeyData(), response);
                    }
                }
                return true;
            }
        }
        return false;
    }

    private void runWithPartitionScan() {
        // if we reach here, it means we didn't manage to leverage index and we fall-back to full-partition scan
        int totalEntryCount = recordStore.size();
        responses = new MapEntries(totalEntryCount);
        Queue<Object> outComes = null;
        operator = operator(this, entryProcessor, getPredicate());

        Iterator<Record> iterator = recordStore.iterator(Clock.currentTimeMillis(), false);
        while (iterator.hasNext()) {
            Record record = iterator.next();
            Data dataKey = toHeapData(record.getKey());

            Data response = operator.operateOnKey(dataKey).getResult();
            if (response != null) {
                responses.add(dataKey, response);
            }

            EntryEventType eventType = operator.getEventType();
            if (eventType != null) {
                if (outComes == null) {
                    outComes = new LinkedList<Object>();
                }

                outComes.add(dataKey);
                outComes.add(operator.getOldValue());
                outComes.add(operator.getNewValue());
                outComes.add(eventType);
            }
        }

        if (outComes != null) {
            // This iteration is needed to work around an issue related with binary elastic hash map (BEHM).
            // Removal via map#remove() while iterating on BEHM distorts it and we can see some entries remain
            // in the map even we know that iteration is finished. Because in this case, iteration can miss some entries.
            do {
                Data dataKey = (Data) outComes.poll();
                Object oldValue = outComes.poll();
                Object newValue = outComes.poll();
                EntryEventType eventType = (EntryEventType) outComes.poll();

                operator.init(dataKey, oldValue, newValue, null, eventType)
                        .doPostOperateOps();

            } while (!outComes.isEmpty());
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
        if (backupProcessor == null) {
            return null;
        }
        if (keysFromIndex != null) {
            // if we used index we leverage it for the backup too
            return new HDMultipleEntryBackupOperation(name, keysFromIndex, backupProcessor);
        } else {
            // if no index used we will do a full partition-scan on backup too
            return new HDPartitionWideEntryBackupOperation(name, backupProcessor);
        }
    }

    @Override
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
