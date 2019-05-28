package com.hazelcast.internal.serialization.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EnterpriseClusterVersionListenerTest {

    private static final Version V3_6 = Version.of(3, 6);

    private EnterpriseClusterVersionListener listener = new EnterpriseClusterVersionListener();

    @Test
    public void uninitialized() {
        assertEquals(Version.UNKNOWN, listener.getClusterVersion());
    }

    @Test
    public void initialized() {
        listener.onClusterVersionChange(V3_6);

        assertEquals(V3_6, listener.getClusterVersion());
    }
}
