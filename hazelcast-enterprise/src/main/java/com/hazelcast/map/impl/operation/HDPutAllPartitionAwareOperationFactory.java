package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.spi.Operation;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;
import java.util.Map;

/**
 * Inserts the {@link MapEntries} for all partitions of a member via locally invoked {@link PutAllOperation}.
 * <p>
 * Used to reduce the number of remote invocations of an {@link com.hazelcast.core.IMap#putAll(Map)} call.
 */
public class HDPutAllPartitionAwareOperationFactory extends PutAllPartitionAwareOperationFactory {

    @SuppressWarnings("unused")
    public HDPutAllPartitionAwareOperationFactory() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public HDPutAllPartitionAwareOperationFactory(String name, int[] partitions, MapEntries[] mapEntries) {
        super(name, partitions, mapEntries);
    }

    @Override
    public Operation createPartitionOperation(int partitionId) {
        for (int i = 0; i < partitions.length; i++) {
            if (partitions[i] == partitionId) {
                return new HDPutAllOperation(name, mapEntries[i]);
            }
        }
        throw new IllegalArgumentException("Unknown partitionId " + partitionId + " (" + Arrays.toString(partitions) + ")");
    }

    @Override
    public int getFactoryId() {
        return EnterpriseMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.PUT_ALL_PARTITION_AWARE_FACTORY;
    }
}
