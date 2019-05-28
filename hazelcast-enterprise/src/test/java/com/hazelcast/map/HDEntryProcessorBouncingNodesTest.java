package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.HDTestSupport.getHDConfig;

/**
 * Creates a map that is used to test data consistency while nodes are joining and leaving the cluster.
 * <p>
 * The basic idea is pretty simple. We'll add a number to a list for each key in the IMap. This allows us to verify whether
 * the numbers are added in the correct order and also whether there's any data loss as nodes leave or join the cluster.
 */
@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HDEntryProcessorBouncingNodesTest extends EntryProcessorBouncingNodesTest {

    @Override
    protected Config getConfig() {
        return getHDConfig();
    }
}
