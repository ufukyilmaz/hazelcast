package com.hazelcast.map.impl.operation;

import com.hazelcast.enterprise.wan.operation.MerkleTreeNodeValueComparison;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;

import java.io.IOException;

/**
 * Factory for creating instances of {@link MerkleTreeNodeCompareOperation}.
 *
 * @since 3.11
 */
public class MerkleTreeNodeCompareOperationFactory implements OperationFactory {

    private String name;
    private MerkleTreeNodeValueComparison remoteNodes;

    public MerkleTreeNodeCompareOperationFactory() {
    }

    public MerkleTreeNodeCompareOperationFactory(String name, MerkleTreeNodeValueComparison remoteNodes) {
        this.name = name;
        this.remoteNodes = remoteNodes;
    }

    @Override
    public Operation createOperation() {
        return new MerkleTreeNodeCompareOperation(name, remoteNodes);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeObject(remoteNodes);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        remoteNodes = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return EnterpriseMapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return EnterpriseMapDataSerializerHook.MERKLE_TREE_NODE_COMPARE_OPERATION_FACTORY;
    }
}
