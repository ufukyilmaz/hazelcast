package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.ArrayDataSerializableFactory;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.ConstructorFunction;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_MAP_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.ENTERPRISE_MAP_DS_FACTORY_ID;

public final class EnterpriseMapDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(ENTERPRISE_MAP_DS_FACTORY, ENTERPRISE_MAP_DS_FACTORY_ID);

    public static final int MERKLE_TREE_NODE_COMPARE_OPERATION = 1;
    public static final int MERKLE_TREE_NODE_COMPARE_OPERATION_FACTORY = 2;
    public static final int MERKLE_TREE_GET_ENTRIES_OPERATION = 3;
    public static final int MERKLE_TREE_GET_ENTRY_COUNT_OPERATION = 4;

    private static final int LEN = MERKLE_TREE_GET_ENTRY_COUNT_OPERATION + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        ConstructorFunction<Integer, IdentifiedDataSerializable>[] constructors = new ConstructorFunction[LEN];

        constructors[MERKLE_TREE_NODE_COMPARE_OPERATION] = arg -> new MerkleTreeNodeCompareOperation();
        constructors[MERKLE_TREE_NODE_COMPARE_OPERATION_FACTORY] = arg -> new MerkleTreeNodeCompareOperationFactory();
        constructors[MERKLE_TREE_GET_ENTRIES_OPERATION] = arg -> new MerkleTreeGetEntriesOperation();
        constructors[MERKLE_TREE_GET_ENTRY_COUNT_OPERATION] = arg -> new MerkleTreeGetEntryCountOperation();

        return new ArrayDataSerializableFactory(constructors);
    }
}
