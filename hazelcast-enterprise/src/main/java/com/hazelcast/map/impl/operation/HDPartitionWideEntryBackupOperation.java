package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import static com.hazelcast.internal.nearcache.impl.invalidation.ToHeapDataConverter.toHeapData;
import static com.hazelcast.map.impl.operation.EntryOperator.operator;

public class HDPartitionWideEntryBackupOperation extends AbstractHDMultipleEntryOperation implements BackupOperation {

    public HDPartitionWideEntryBackupOperation() {
    }

    public HDPartitionWideEntryBackupOperation(String name, EntryBackupProcessor backupProcessor) {
        super(name, backupProcessor);
    }

    @Override
    protected void runInternal() {
        int totalEntryCount = recordStore.size();
        responses = new MapEntries(totalEntryCount);
        Queue<Object> outComes = null;
        EntryOperator operator = operator(this, backupProcessor, getPredicate(), true);

        Iterator<Record> iterator = recordStore.iterator(Clock.currentTimeMillis(), true);
        while (iterator.hasNext()) {
            Record record = iterator.next();
            Data dataKey = toHeapData(record.getKey());
            operator.operateOnKey(dataKey);

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
        return true;
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
