package com.hazelcast.client.quorum;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HiDensityClientCacheReadQuorumTest
        extends HiDensityClientCacheQuorumTestSupport {

    @BeforeClass
    public static void init()
            throws InterruptedException {
        HiDensityClientCacheQuorumTestSupport.initialize(QuorumType.READ);
    }

    @Test
    public void testGetOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        cache1.get(1);
    }

    @Test(expected = QuorumException.class)
    public void testGetOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        cache4.get(1);
    }

    @Test
    public void testContainsOperationSuccessfulWhenQuorumSizeMet() {
        cache1.containsKey(1);
    }

    @Test(expected = QuorumException.class)
    public void testContainsOperationThrowsExceptionWhenQuorumSizeNotMet() {
        cache4.containsKey(1);
    }

    @Test
    public void testGetAllOperationSuccessfulWhenQuorumSizeMet() {
        HashSet<Integer> hashSet = new HashSet<Integer>();
        hashSet.add(123);
        cache1.getAll(hashSet);
    }

    @Test(expected = QuorumException.class)
    public void testGetAllOperationThrowsExceptionWhenQuorumSizeNotMet() {
        HashSet<Integer> hashSet = new HashSet<Integer>();
        hashSet.add(123);
        cache4.getAll(hashSet);
    }

    @Test
    public void testIteratorOperationSuccessfulWhenQuorumSizeMet() {
        cache1.iterator();
    }

    @Test(expected = QuorumException.class)
    public void testIteratorOperationThrowsExceptionWhenQuorumSizeNotMet() {
        cache4.iterator();
    }

    @Test
    public void testGetAsyncOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        Future<String> foo = cache1.getAsync(1);
        foo.get();
    }

    @Test(expected = ExecutionException.class)
    public void testGetAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<String> foo = cache4.getAsync(1);
        foo.get();
    }

    @Test
    public void testPutGetWhenQuorumSizeMet() {
        cache1.put(123, "foo");
        assertEquals("foo", cache2.get(123));
    }

    @Test
    public void testPutRemoveGetShouldReturnNullWhenQuorumSizeMet() {
        cache1.put(123, "foo");
        cache1.remove(123);
        assertNull(cache2.get(123));
    }
}
