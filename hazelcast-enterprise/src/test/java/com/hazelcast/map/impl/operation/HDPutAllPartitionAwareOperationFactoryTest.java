package com.hazelcast.map.impl.operation;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDPutAllPartitionAwareOperationFactoryTest extends PutAllPartitionAwareOperationFactoryTest {

    @Override
    protected PutAllPartitionAwareOperationFactory getFactory(String name, int[] partitions, MapEntries[] mapEntries) {
        return new HDPutAllPartitionAwareOperationFactory(name, partitions, mapEntries);
    }
}
