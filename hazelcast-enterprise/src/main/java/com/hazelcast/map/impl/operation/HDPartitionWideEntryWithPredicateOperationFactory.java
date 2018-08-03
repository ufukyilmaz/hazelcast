package com.hazelcast.map.impl.operation;

import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;

import java.io.IOException;

public class HDPartitionWideEntryWithPredicateOperationFactory extends PartitionAwareOperationFactory {

    private String name;
    private EntryProcessor entryProcessor;
    private Predicate predicate;

    public HDPartitionWideEntryWithPredicateOperationFactory() {
    }

    public HDPartitionWideEntryWithPredicateOperationFactory(String name, EntryProcessor entryProcessor, Predicate predicate) {
        this.name = name;
        this.entryProcessor = entryProcessor;
        this.predicate = predicate;
    }

    @Override
    public PartitionAwareOperationFactory createFactoryOnRunner(NodeEngine nodeEngine, int[] partitions) {
        return new HDPartitionWideEntryWithPredicateOperationFactory(name, entryProcessor, predicate);
    }

    @Override
    public Operation createPartitionOperation(int partition) {
        return new HDPartitionWideEntryWithPredicateOperation(name, entryProcessor, predicate);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeObject(entryProcessor);
        out.writeObject(predicate);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        entryProcessor = in.readObject();
        predicate = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return EnterpriseMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.PARTITION_WIDE_PREDICATE_ENTRY_FACTORY;
    }
}
