package com.hazelcast.cache.hidensity.operation;

import com.hazelcast.cache.HazelcastExpiryPolicy;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.expiry.ExpiryPolicy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.hidensity.operation.AbstractHDCacheOperationTest.OperationType.PUT_ALL;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CachePutAllOperationTest extends AbstractHDCacheOperationTest {

    private static final String CACHE_NAME = "CachePutAllOperationTest";

    private List<Map.Entry<Data, Data>> entries;

    @Before
    @Override
    public void setUp() {
        super.setUp();

        entries = createEntries(ENTRY_COUNT);
    }

    @Test
    public void testRun() throws Exception {
        syncBackupCount = 0;
        throwNativeOOME = false;

        testRunInternal();
    }

    @Test
    public void testRun_shouldWriteAllBackups() throws Exception {
        syncBackupCount = 1;
        throwNativeOOME = false;

        testRunInternal();
    }

    @Test
    public void testRun_whenNativeOutOfMemoryError_thenShouldNotInsertEntriesTwice() throws Exception {
        syncBackupCount = 0;
        throwNativeOOME = true;

        testRunInternal();
    }

    @Test
    public void testRun_whenNativeOutOfMemoryError_thenShouldNotInsertEntriesTwice_shouldWriteAllBackups() throws Exception {
        syncBackupCount = 1;
        throwNativeOOME = true;

        testRunInternal();
    }

    private void testRunInternal() throws Exception {
        configureBackups();
        configureRecordStore(PUT_ALL);

        ExpiryPolicy expiryPolicy = new HazelcastExpiryPolicy(1, 1, 1, TimeUnit.HOURS);

        // CachePutAllOperation
        CachePutAllOperation operation = new CachePutAllOperation(CACHE_NAME, entries, expiryPolicy);
        executeOperation(operation, PARTITION_ID);
        assertBackupConfiguration(operation);

        verifyRecordStoreAfterRun(PUT_ALL, false);
        verifyForcedEviction();

        // CachePutAllBackupOperation
        if (syncBackupCount > 0) {
            executeOperation(operation.getBackupOperation(), PARTITION_ID);

            verifyRecordStoreAfterRun(PUT_ALL, true);
            verifyForcedEviction();
        }
    }

    @Override
    String getCacheName() {
        return CACHE_NAME;
    }
}
