package com.hazelcast.map.impl.operation;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

abstract class AbstractHDMultipleEntryOperation extends HDMapOperation implements MutatingOperation, PartitionAwareOperation {

    protected MapEntries responses;
    protected EntryProcessor entryProcessor;
    protected EntryBackupProcessor backupProcessor;
    protected List<WanEventHolder> wanEventList = Collections.emptyList();

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

    protected Predicate getPredicate() {
        return null;
    }

    public void setWanEventList(List<WanEventHolder> wanEventList) {
        assert wanEventList != null;

        this.wanEventList = wanEventList;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(wanEventList.size());
        for (WanEventHolder wanEventWrapper : wanEventList) {
            out.writeData(wanEventWrapper.getKey());
            out.writeData(wanEventWrapper.getValue());
            out.writeInt(wanEventWrapper.getEventType().getType());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        if (size > 0) {
            wanEventList = new ArrayList<WanEventHolder>(size);
            for (int i = 0; i < size; i++) {
                Data key = in.readData();
                Data value = in.readData();
                EntryEventType entryEventType = EntryEventType.getByType(in.readInt());
                wanEventList.add(new WanEventHolder(key, value, entryEventType));
            }
        }
    }
}
