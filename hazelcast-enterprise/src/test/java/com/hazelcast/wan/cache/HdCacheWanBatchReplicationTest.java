package com.hazelcast.wan.cache;

import com.hazelcast.cache.jsr.JsrTestUtil;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.wan.replication.WanBatchReplication;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class})
public class HdCacheWanBatchReplicationTest extends AbstractCacheWanReplicationTest {

    @BeforeClass
    public static void initJCache() {
        JsrTestUtil.setup();
    }

    @AfterClass
    public static void cleanupJCache() {
        JsrTestUtil.cleanup();
    }

    @Override
    public String getReplicationImpl() {
        return WanBatchReplication.class.getName();
    }

    @Override
    public InMemoryFormat getMemoryFormat() {
        return InMemoryFormat.NATIVE;
    }
}
