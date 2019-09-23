package com.hazelcast.json.internal;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.serialization.impl.DataInputNavigableJsonAdapter;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.NavigableJsonInputAdapter;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.internal.memory.StandardMemoryManager;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NativeJsonSchemaHelperTest extends JsonSchemaHelperTest {

    private EnterpriseSerializationService ss = new EnterpriseSerializationServiceBuilder().build();

    private MemoryAllocator memoryAllocator = new StandardMemoryManager(new MemorySize(16, MemoryUnit.MEGABYTES));

    @Override
    protected NavigableJsonInputAdapter toAdapter(HazelcastJsonValue jsonValue) {
        return new DataInputNavigableJsonAdapter(ss.createObjectDataInput(ss.toNativeData(jsonValue, memoryAllocator)), 12);
    }
}
