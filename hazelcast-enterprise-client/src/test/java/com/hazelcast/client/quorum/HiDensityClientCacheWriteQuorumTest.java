package com.hazelcast.client.quorum;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HiDensityClientCacheWriteQuorumTest extends HiDensityClientCacheQuorumTestSupport {

    @BeforeClass
    public static void init() throws Exception {
        HiDensityClientCacheQuorumTestSupport.initialize(QuorumType.WRITE);
    }

    @Test
    public void testPutOperationSuccessfulWhenQuorumSizeMet() {
        cache1.put(1, "");
    }

    @Test(expected = QuorumException.class)
    public void testPutOperationThrowsExceptionWhenQuorumSizeNotMet() {
        cache4.put(1, "");
    }

    @Test
    public void testGetAndPutOperationSuccessfulWhenQuorumSizeMet() {
        cache1.getAndPut(1, "");
    }

    @Test(expected = QuorumException.class)
    public void testGetAndPutOperationThrowsExceptionWhenQuorumSizeNotMet() {
        cache4.getAndPut(1, "");
    }

    @Test
    public void testGetAndRemoveOperationSuccessfulWhenQuorumSizeMet() {
        cache1.getAndRemove(1);
    }

    @Test(expected = QuorumException.class)
    public void testGetAndRemoveOperationThrowsExceptionWhenQuorumSizeNotMet() {
        cache4.getAndRemove(1);
    }

    @Test
    public void testGetAndReplaceOperationSuccessfulWhenQuorumSizeMet() {
        cache1.getAndReplace(1, "");
    }

    @Test(expected = QuorumException.class)
    public void testGetAndReplaceOperationThrowsExceptionWhenQuorumSizeNotMet() {
        cache4.getAndReplace(1, "");
    }

    @Test
    public void testClearOperationSuccessfulWhenQuorumSizeMet() {
        cache1.clear();
    }

    @Test(expected = QuorumException.class)
    public void testClearOperationThrowsExceptionWhenQuorumSizeNotMet() {
        cache4.clear();
    }

    @Test
    public void testPutIfAbsentOperationSuccessfulWhenQuorumSizeMet() {
        cache1.putIfAbsent(1, "");
    }

    @Test(expected = QuorumException.class)
    public void testPutIfAbsentOperationThrowsExceptionWhenQuorumSizeNotMet() {
        cache4.putIfAbsent(1, "");
    }

    @Test
    public void testPutAllOperationSuccessfulWhenQuorumSizeMet() {
        HashMap<Integer, String> hashMap = new HashMap<Integer, String>();
        hashMap.put(123, "");
        cache1.putAll(hashMap);
    }

    @Test(expected = QuorumException.class)
    public void testPutAllOperationThrowsExceptionWhenQuorumSizeNotMet() {
        HashMap<Integer, String> hashMap = new HashMap<Integer, String>();
        hashMap.put(123, "");
        cache4.putAll(hashMap);
    }

    @Test
    public void testRemoveOperationSuccessfulWhenQuorumSizeMet() {
        cache1.remove(1);
    }

    @Test(expected = QuorumException.class)
    public void testRemoveOperationThrowsExceptionWhenQuorumSizeNotMet() {
        cache4.remove(1);
    }

    @Test
    public void testRemoveAllOperationSuccessfulWhenQuorumSizeMet() {
        cache1.removeAll();
    }

    @Test(expected = QuorumException.class)
    public void testRemoveAllOperationThrowsExceptionWhenQuorumSizeNotMet() {
        cache4.removeAll();
    }

    @Test
    public void testReplaceOperationSuccessfulWhenQuorumSizeMet() {
        cache1.replace(1, "");
    }

    @Test(expected = QuorumException.class)
    public void testReplaceOperationThrowsExceptionWhenQuorumSizeNotMet() {
        cache4.replace(1, "");
    }

    @Test
    public void testGetAndPutAsyncOperationSuccessfulWhenQuorumSizeMet() {
        cache1.getAndPutAsync(1, "");
    }

    @Test(expected = ExecutionException.class)
    public void testGetAndPutAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<String> foo = cache4.getAndPutAsync(1, "");
        foo.get();
    }

    @Test
    public void testGetAndRemoveAsyncOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        Future<String> foo = cache1.getAndRemoveAsync(1);
        foo.get();
    }

    @Test(expected = ExecutionException.class)
    public void testGetAndRemoveAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<String> foo = cache4.getAndRemoveAsync(1);
        foo.get();
    }

    @Test
    public void testGetAndReplaceAsyncOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        Future<String> foo = cache1.getAndReplaceAsync(1, "");
        foo.get();
    }

    @Test(expected = ExecutionException.class)
    public void testGetAndReplaceAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<String> foo = cache4.getAndReplaceAsync(1, "");
        foo.get();
    }

    @Test
    public void testInvokeOperationSuccessfulWhenQuorumSizeMet() {
        cache1.invoke(123, new SimpleEntryProcessor());
    }

    @Test(expected = EntryProcessorException.class)
    public void testInvokeOperationThrowsExceptionWhenQuorumSizeNotMet() {
        cache4.invoke(123, new SimpleEntryProcessor());
    }

    @Test
    public void testInvokeAllOperationSuccessfulWhenQuorumSizeMet() {
        HashSet<Integer> hashSet = new HashSet<Integer>();
        hashSet.add(123);
        EntryProcessorResult epr = cache1.invokeAll(hashSet, new SimpleEntryProcessor()).get(123);
        assertNull(epr);

    }

    @Test(expected = EntryProcessorException.class)
    public void testInvokeAllOperationThrowsExceptionWhenQuorumSizeNotMet() {
        HashSet<Integer> hashSet = new HashSet<Integer>();
        hashSet.add(123);
        cache4.invokeAll(hashSet, new SimpleEntryProcessor()).get(123).get();
    }

    @Test
    public void testPutAsyncOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        Future<Void> foo = cache1.putAsync(1, "");
        foo.get();
    }

    @Test(expected = ExecutionException.class)
    public void testPutAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<Void> foo = cache4.putAsync(1, "");
        foo.get();
    }

    @Test
    public void testPutIfAbsentAsyncOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        Future<Boolean> foo = cache1.putIfAbsentAsync(1, "");
        foo.get();
    }

    @Test(expected = ExecutionException.class)
    public void testPutIfAbsentAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<Boolean> foo = cache4.putIfAbsentAsync(1, "");
        foo.get();
    }

    @Test
    public void testRemoveAsyncOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        Future<Boolean> foo = cache1.removeAsync(1);
        foo.get();
    }

    @Test(expected = ExecutionException.class)
    public void testRemoveAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<Boolean> foo = cache4.removeAsync(1);
        foo.get();
    }

    @Test
    public void testReplaceAsyncOperationSuccessfulWhenQuorumSizeMet() throws Exception {
        Future<Boolean> foo = cache1.replaceAsync(1, "");
        foo.get();
    }

    @Test(expected = ExecutionException.class)
    public void testReplaceAsyncOperationThrowsExceptionWhenQuorumSizeNotMet() throws Exception {
        Future<Boolean> foo = cache4.replaceAsync(1, "");
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

    public static class SimpleEntryProcessor implements EntryProcessor<Integer, String, Void>, Serializable {

        private static final long serialVersionUID = -396575576353368113L;

        @Override
        public Void process(MutableEntry<Integer, String> entry, Object... arguments) throws EntryProcessorException {

            entry.setValue("Foo");
            return null;
        }
    }
}
