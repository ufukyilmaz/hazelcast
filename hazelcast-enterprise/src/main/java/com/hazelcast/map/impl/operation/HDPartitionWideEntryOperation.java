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
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import static com.hazelcast.internal.nearcache.impl.invalidation.ToHeapDataConverter.toHeapData;
import static com.hazelcast.map.impl.operation.EntryOperator.operator;

/**
 * GOTCHA : This operation does NOT load missing keys from map-store for now.
 */
public class HDPartitionWideEntryOperation extends AbstractHDMultipleEntryOperation implements BackupAwareOperation {

    protected transient EntryOperator operator;

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
        int totalEntryCount = recordStore.size();
        responses = new MapEntries(totalEntryCount);
        Queue<Object> outComes = null;
        operator = operator(this, entryProcessor, getPredicate(), true);

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
            // This iteration is needed to work around an issue related with binary elastic hash map(BEHM).
            // Removal via map#remove while iterating on BEHM distortes it and we can see some entries are remained
            // in map even we know that iteration is finished. Because in this case, iteration can miss some entries.
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
        HDPartitionWideEntryBackupOperation operation = new HDPartitionWideEntryBackupOperation(name, backupProcessor);
        operation.setWanEventList(operator.getWanEventList());
        return operation;
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
