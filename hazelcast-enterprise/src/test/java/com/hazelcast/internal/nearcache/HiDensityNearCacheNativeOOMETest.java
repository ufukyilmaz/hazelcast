package com.hazelcast.internal.nearcache;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.verification.VerificationMode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class HiDensityNearCacheNativeOOMETest extends HazelcastTestSupport {

    private static final String KEY = "key";
    private static final String VALUE = "value";
    private static final NativeOutOfMemoryError NATIVE_OOME = new NativeOutOfMemoryError();
    private static final MemorySize DEFAULT_MEMORY_SIZE = new MemorySize(256, MemoryUnit.MEGABYTES);

    private Collection<NearCache> nearCaches = new ArrayList<NearCache>();

    private NearCacheRecordStore<String, String> nearCacheRecordStore;
    private NearCacheConfig nearCacheConfig;
    private NearCacheManager nearCacheManager;
    private PoolingMemoryManager memoryManager;
    private EnterpriseSerializationService serializationService;
    private ExecutionService executionService;
    private HazelcastProperties properties;
    private Data keyData;
    private Data valueData;

    private HiDensityNearCache<String, String> nearCache;

    @Before
    public void setUp() {
        nearCacheRecordStore = createNearCacheRecordStore();

        nearCacheConfig = new NearCacheConfig()
                .setInMemoryFormat(InMemoryFormat.NATIVE);

        nearCacheManager = mock(NearCacheManager.class);
        when(nearCacheManager.listAllNearCaches()).thenReturn(nearCaches);

        memoryManager = new PoolingMemoryManager(DEFAULT_MEMORY_SIZE);
        memoryManager.registerThread(Thread.currentThread());

        serializationService = new EnterpriseSerializationServiceBuilder()
                .setMemoryManager(memoryManager)
                .build();

        executionService = mock(ExecutionService.class);
        properties = new HazelcastProperties(new Properties());

        keyData = serializationService.toData(KEY);
        valueData = serializationService.toData(VALUE);
    }

    @After
    public void tearDown() {
        memoryManager.dispose();
    }

    @Test
    public void putWithoutNativeOOME() {
        createNearCache(nearCacheManager);

        nearCache.put(KEY, keyData, VALUE, valueData);

        // recordStore.put() succeeds on the first try
        verifyPutOnRecordStore(1);
        verifyNoMoreInteractions(nearCacheRecordStore);
    }

    @Test
    public void putWithNativeOOME_withSuccessfulEvictionOnSameNearCache() {
        createNearCache(nearCacheManager);
        doThrow(NATIVE_OOME).doNothing().when(nearCacheRecordStore).put(eq(KEY), eq(keyData), eq(VALUE), eq(valueData));
        doReturn(1).when(nearCacheRecordStore).size();

        nearCache.put(KEY, keyData, VALUE, valueData);

        // recordStore.put() succeeds after the first forced eviction on the same Near Cache
        verifyPutOnRecordStore(2);
        verifyForcedEviction(nearCacheRecordStore, times(1));
        verifyNoMoreInteractions(nearCacheRecordStore);
    }

    @Test
    public void putWithNativeOOME_afterEvictionOnSameNearCache() {
        createNearCache(nearCacheManager);
        doThrow(NATIVE_OOME).doThrow(NATIVE_OOME).doNothing().when(nearCacheRecordStore).put(eq(KEY), eq(keyData),
                eq(VALUE), eq(valueData));
        doReturn(1).when(nearCacheRecordStore).size();

        nearCache.put(KEY, keyData, VALUE, valueData);

        // recordStore.put() succeeds in the second loop after the first forced eviction didn't work
        verifyPutOnRecordStore(3);
        verifyForcedEviction(nearCacheRecordStore, times(2));
        verifyNoMoreInteractions(nearCacheRecordStore);
    }

    @Test
    public void putWithNativeOOME_withSuccessfulEvictionOnOtherNearCache() {
        createNearCache(nearCacheManager);
        doThrow(NATIVE_OOME).doNothing().when(nearCacheRecordStore).put(eq(KEY), eq(keyData), eq(VALUE), eq(valueData));
        doReturn(0).when(nearCacheRecordStore).size();

        NearCacheRecordStore<String, String> otherNearCacheRecordStore = addNearCacheToNearCacheManager();
        doReturn(1).when(otherNearCacheRecordStore).size();

        nearCache.put(KEY, keyData, VALUE, valueData);

        // recordStore.put() succeeds after the first forced eviction on the other Near Cache
        verifyPutOnRecordStore(2);
        verify(nearCacheRecordStore).size();
        verifyNoMoreInteractions(nearCacheRecordStore);

        verifyForcedEviction(otherNearCacheRecordStore, times(1));
        verifyNoMoreInteractions(otherNearCacheRecordStore);
    }

    @Test
    public void putWithNativeOOME_withUnsuccessfulEvictionOnOtherNearCache() {
        createNearCache(nearCacheManager);
        doThrow(NATIVE_OOME).doThrow(NATIVE_OOME).doNothing().when(nearCacheRecordStore).put(eq(KEY), eq(keyData),
                eq(VALUE), eq(valueData));
        doReturn(0).when(nearCacheRecordStore).size();

        NearCacheRecordStore<String, String> otherNearCacheRecordStore = addNearCacheToNearCacheManager();
        doReturn(1).when(otherNearCacheRecordStore).size();

        nearCache.put(KEY, keyData, VALUE, valueData);

        // recordStore.put() succeeds in the second loop after the first forced eviction on the other Near Cache didn't work
        verifyPutOnRecordStore(3);
        verify(nearCacheRecordStore, times(2)).size();
        verifyNoMoreInteractions(nearCacheRecordStore);

        verifyForcedEviction(otherNearCacheRecordStore, times(2));
        verifyNoMoreInteractions(otherNearCacheRecordStore);
    }

    @Test
    public void putWithNativeOOME_noCandidatesForEviction_afterSuccessfulCompaction() {
        createNearCache(nearCacheManager);
        doThrow(NATIVE_OOME).doNothing().when(nearCacheRecordStore).put(eq(KEY), eq(keyData), eq(VALUE), eq(valueData));
        doReturn(0).when(nearCacheRecordStore).size();

        NearCacheRecordStore<String, String> otherNearCacheRecordStore = addNearCacheToNearCacheManager();
        doReturn(0).when(otherNearCacheRecordStore).size();

        nearCache.put(KEY, keyData, VALUE, valueData);

        // recordStore.put() succeeds in the second loop after there were no Near Cache candidates for eviction,
        // so a memory compaction was done
        verifyPutOnRecordStore(2);
        verify(nearCacheRecordStore).size();
        verifyNoMoreInteractions(nearCacheRecordStore);

        verify(otherNearCacheRecordStore).size();
        verifyNoMoreInteractions(otherNearCacheRecordStore);
    }

    @Test
    public void putWithNativeOOME_removeKey_afterUnsuccessfulMemoryCompaction() {
        createNearCache(nearCacheManager);
        doThrow(NATIVE_OOME).when(nearCacheRecordStore).put(eq(KEY), eq(keyData), eq(VALUE), eq(valueData));
        doReturn(0).when(nearCacheRecordStore).size();

        nearCache.put(KEY, keyData, VALUE, valueData);

        // recordStore.put() doesn't succeed at all and we finally give up by removing the actual key from the RecordStore
        verifyPutOnRecordStore(2);
        verify(nearCacheRecordStore, times(2)).size();
        verify(nearCacheRecordStore).invalidate(eq(KEY));
        verifyNoMoreInteractions(nearCacheRecordStore);
    }

    private void createNearCache(NearCacheManager nearCacheManager) {
        nearCache = createNearCache("myNearCache", nearCacheManager, nearCacheRecordStore);
    }

    private HiDensityNearCache<String, String> createNearCache(String name, NearCacheManager nearCacheManager,
                                                               NearCacheRecordStore<String, String> recordStore) {
        HiDensityNearCache<String, String> nearCache = new HiDensityNearCache<String, String>(name, nearCacheConfig,
                nearCacheManager, recordStore, serializationService,
                executionService.getGlobalTaskScheduler(), null, properties);
        nearCache.setMemoryManager(memoryManager);

        nearCaches.add(nearCache);

        return nearCache;
    }

    private NearCacheRecordStore<String, String> addNearCacheToNearCacheManager() {
        NearCacheRecordStore<String, String> recordStore = createNearCacheRecordStore();
        createNearCache("anotherNearCache", nearCacheManager, recordStore);
        return recordStore;
    }

    private void verifyPutOnRecordStore(int times) {
        verify(nearCacheRecordStore, times(times)).doEvictionIfRequired();
        verify(nearCacheRecordStore, times(times)).put(eq(KEY), eq(keyData), eq(VALUE), eq(valueData));
    }

    private void verifyForcedEviction(NearCacheRecordStore<String, String> recordStore, VerificationMode verificationMode) {
        verify(recordStore, verificationMode).size();
        verify(recordStore, verificationMode).doEviction();
    }

    @SuppressWarnings("unchecked")
    private static NearCacheRecordStore<String, String> createNearCacheRecordStore() {
        return mock(NearCacheRecordStore.class);
    }
}
