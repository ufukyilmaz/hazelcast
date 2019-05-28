package com.hazelcast.json;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MetadataPolicy;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.test.TestTaskExecutorUtil;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDMapIndexJsonTest extends MapIndexJsonTest {

    @Parameterized.Parameters(name = "inMemoryFormat: {0}, metadataPolicy: {1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][] {
                {InMemoryFormat.NATIVE, MetadataPolicy.OFF},
                {InMemoryFormat.NATIVE, MetadataPolicy.CREATE_ON_UPDATE},
        });
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();

        NativeMemoryConfig memoryConfig = new NativeMemoryConfig().setEnabled(true);
        config.setNativeMemoryConfig(memoryConfig);
        return config;
    }

    @Override
    protected Set<QueryableEntry> getRecordsFromInternalIndex(Collection<HazelcastInstance> instances, String mapName, String attribute, final Comparable value) {
        Set<QueryableEntry> records = new HashSet<QueryableEntry>();
        for (HazelcastInstance instance: instances) {
            List<Index> indexes = getIndexOfAttributeForMap(instance, mapName, attribute);
            for (final Index index : indexes) {
                Set<QueryableEntry> set = TestTaskExecutorUtil.runOnPartitionThread(instance, new Callable<Set<QueryableEntry>>() {
                    @Override
                    public Set<QueryableEntry> call() throws Exception {
                        return index.getRecords(value);
                    }
                }, 0); // for testing purposes any partition thread is fine
                records.addAll(set);
            }
        }
        return records;
    }
}
