package com.hazelcast.internal.serialization.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.ClusterVersion;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class EnterpriseClusterVersionListenerTest {

    private static ClusterVersion V3_6 = new ClusterVersion(3, 6);

    private EnterpriseClusterVersionListener listener = new EnterpriseClusterVersionListener();

    @Test
    public void uninitialized() {
        assertEquals(ClusterVersion.UNKNOWN, listener.getClusterVersion());
    }

    @Test
    public void initialized() {
        listener.onClusterVersionChange(V3_6);

        assertEquals(V3_6, listener.getClusterVersion());
    }

}