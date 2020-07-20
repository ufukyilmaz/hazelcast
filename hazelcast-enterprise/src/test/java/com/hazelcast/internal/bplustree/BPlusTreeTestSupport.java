package com.hazelcast.internal.bplustree;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.internal.elastic.tree.MapEntryFactory;
import com.hazelcast.internal.memory.FreeMemoryChecker;
import com.hazelcast.internal.memory.GlobalIndexPoolingAllocator;
import com.hazelcast.internal.memory.HazelcastMemoryManager;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.PoolingMemoryManager;
import com.hazelcast.internal.memory.impl.UnsafeMallocFactory;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.query.impl.CachedQueryEntry;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import static com.hazelcast.config.NativeMemoryConfig.DEFAULT_METADATA_SPACE_PERCENTAGE;
import static com.hazelcast.config.NativeMemoryConfig.DEFAULT_MIN_BLOCK_SIZE;
import static com.hazelcast.config.NativeMemoryConfig.DEFAULT_PAGE_SIZE;
import static com.hazelcast.internal.bplustree.HDBPlusTree.DEFAULT_BPLUS_TREE_SCAN_BATCH_MAX_SIZE;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.getKeysCount;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.getLockState;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.getNodeLevel;
import static com.hazelcast.internal.bplustree.HDLockManager.getReadWaitersCount;
import static com.hazelcast.internal.bplustree.HDLockManager.getUsersCount;
import static com.hazelcast.internal.bplustree.HDLockManager.getWriteWaitersCount;
import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.AMEM;
import static com.hazelcast.internal.memory.MemoryAllocator.NULL_ADDRESS;
import static com.hazelcast.internal.serialization.DataType.HEAP;
import static com.hazelcast.internal.serialization.DataType.NATIVE;
import static com.hazelcast.internal.util.QuickMath.nextPowerOfTwo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BPlusTreeTestSupport extends HazelcastTestSupport {

    private static final int NODE_SIZE = 256;
    static final LockManager MOCKED_LOCK_MANAGER = new MockedLockManager();

    protected HDBPlusTree btree;
    protected long rootAddr = NULL_ADDRESS;
    protected HDBTreeInnerNodeAccessor innerNodeAccessor;
    protected HDBTreeLeafNodeAccessor leafNodeAccessor;

    protected DelegatingMemoryAllocator delegatingIndexAllocator;
    protected AllocatorCallback allocatorCallback;
    protected VersionedLong maAllocateAddr = new VersionedLong();
    protected VersionedLong maFreeAddr = new VersionedLong();
    protected VersionedLong maSize = new VersionedLong();

    protected HazelcastMemoryManager keyAllocator;
    protected EnterpriseSerializationService ess;

    @Before
    public void setUp() {
        PoolingMemoryManager poolingMemoryManager = new PoolingMemoryManager(
                getMemorySize(), DEFAULT_MIN_BLOCK_SIZE, DEFAULT_PAGE_SIZE, getMetadataPercentage(),
                getNodeSize(), new UnsafeMallocFactory(new FreeMemoryChecker()));
        this.keyAllocator = newKeyAllocator(poolingMemoryManager);
        MemoryAllocator indexAllocator = poolingMemoryManager.getGlobalIndexAllocator();
        this.ess = getSerializationService();
        MapEntryFactory factory = new OnHeapEntryFactory(ess, null);
        BPlusTreeKeyComparator keyComparator = newBPlusTreeKeyComparator();
        BPlusTreeKeyAccessor keyAccessor = newBPlusTreeKeyAccessor();
        NodeSplitStrategy splitStrategy = new DefaultNodeSplitStrategy();
        allocatorCallback = new AllocatorCallback(maAllocateAddr, maFreeAddr, maSize);
        delegatingIndexAllocator = newDelegatingIndexMemoryAllocator(indexAllocator, allocatorCallback);
        LockManager lockManager = newLockManager();
        btree = newBPlusTree(ess, keyAllocator, delegatingIndexAllocator, lockManager, keyComparator, keyAccessor,
                factory, getNodeSize(), DEFAULT_BPLUS_TREE_SCAN_BATCH_MAX_SIZE);
        rootAddr = btree.getRoot();
        this.innerNodeAccessor = new HDBTreeInnerNodeAccessor(lockManager, ess, keyComparator, keyAccessor, keyAllocator, delegatingIndexAllocator, getNodeSize(), splitStrategy);
        this.leafNodeAccessor = new HDBTreeLeafNodeAccessor(lockManager, ess, keyComparator, keyAccessor, keyAllocator, delegatingIndexAllocator, getNodeSize(), splitStrategy);
    }

    int getNodeSize() {
        return NODE_SIZE;
    }

    MemorySize getMemorySize() {
        return new MemorySize(1, MemoryUnit.GIGABYTES);
    }

    float getMetadataPercentage() {
        return DEFAULT_METADATA_SPACE_PERCENTAGE;
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    HDBPlusTree newBPlusTree(EnterpriseSerializationService ess,
                             MemoryAllocator keyAllocator,
                             MemoryAllocator indexAllocator, LockManager lockManager,
                             BPlusTreeKeyComparator keyComparator,
                             BPlusTreeKeyAccessor keyAccessor,
                             MapEntryFactory entryFactory,
                             int nodeSize,
                             int indexScanBatchSize) {
        return HDBPlusTree.newHDBTree(ess, keyAllocator, indexAllocator, lockManager, keyComparator, keyAccessor,
                entryFactory, nodeSize, indexScanBatchSize);
    }

    DelegatingMemoryAllocator newDelegatingIndexMemoryAllocator(MemoryAllocator indexAllocator, AllocatorCallback callback) {
        return new DelegatingMemoryAllocator(indexAllocator, callback);
    }

    HazelcastMemoryManager newKeyAllocator(PoolingMemoryManager poolingMemoryManager) {
        return poolingMemoryManager.getGlobalMemoryManager();
    }

    BPlusTreeKeyComparator newBPlusTreeKeyComparator() {
        return new DefaultBPlusTreeKeyComparator(ess);
    }

    BPlusTreeKeyAccessor newBPlusTreeKeyAccessor() {
        return new DefaultBPlusTreeKeyAccessor(ess);
    }

    @After
    public void tearDown() {
        if (rootAddr != NULL_ADDRESS) {
            assertNoLocksLeft();
            assertNoLocksLeft((GlobalIndexPoolingAllocator) delegatingIndexAllocator.getDelegate());
            assertNoKeys((GlobalIndexPoolingAllocator) delegatingIndexAllocator.getDelegate());
        }
    }

    LockManager newLockManager() {
        int stripesCount = nextPowerOfTwo(Runtime.getRuntime().availableProcessors() * 4);
        return new HDLockManager(stripesCount);
    }

    LockingContext newLockingContext() {
        return new LockingContext();
    }

    private NativeMemoryConfig getMemoryConfig() {
        MemorySize memorySize = new MemorySize(100, MemoryUnit.MEGABYTES);
        return new NativeMemoryConfig()
                .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD)
                .setSize(memorySize).setEnabled(true)
                .setMinBlockSize(16).setPageSize(1 << 20);
    }

    protected EnterpriseSerializationService getSerializationService() {
        return new EnterpriseSerializationServiceBuilder()
                .setMemoryManager(keyAllocator)
                .setAllowUnsafe(true)
                .setUseNativeByteOrder(true)
                .build();
    }

    NativeMemoryData nativeData(Object object) {
        return ess.toData(object, NATIVE);
    }

    static class OnHeapEntryFactory implements MapEntryFactory<QueryableEntry> {

        private final EnterpriseSerializationService ess;
        private final Extractors extractors;

        OnHeapEntryFactory(EnterpriseSerializationService ess, Extractors extractors) {
            this.ess = ess;
            this.extractors = extractors;
        }

        @Override
        public CachedQueryEntry create(Data key, Data value) {
            Data heapData = toHeapData(key);
            Data heapValue = toHeapData(value);
            return new CachedQueryEntry(ess, heapData, heapValue, extractors);
        }

        private Data toHeapData(Data data) {
            if (data instanceof NativeMemoryData) {
                NativeMemoryData nativeMemoryData = (NativeMemoryData) data;
                if (nativeMemoryData.totalSize() == 0) {
                    return null;
                }
                return ess.toData(nativeMemoryData, HEAP);
            }
            return data;
        }

    }

    void assertKeysSorted(long nodeAddr, HDBTreeNodeBaseAccessor nodeAccessor) {
        Integer prevKey = null;
        for (int i = 0; i < getKeysCount(nodeAddr); ++i) {
            Integer key = ess.toObject(nodeAccessor.getIndexKeyHeapData(nodeAddr, i));
            if (prevKey != null) {
                assertTrue(prevKey <= key);
            }
            prevKey = key;
        }
    }

    final class DelegatingMemoryAllocator implements MemoryAllocator {

        private final MemoryAllocator delegate;
        private final MemoryAllocatorEventCallback callback;

        DelegatingMemoryAllocator(MemoryAllocator delegate, MemoryAllocatorEventCallback callback) {
            this.delegate = delegate;
            this.callback = callback;
        }

        MemoryAllocator getDelegate() {
            return delegate;
        }

        @Override
        public long allocate(long size) {
            long addr = delegate.allocate(size);
            if (callback != null) {
                callback.onAllocate(addr, size);
            }

            return addr;
        }

        @Override
        public long reallocate(long address, long currentSize, long newSize) {
            long newAddress = delegate.reallocate(address, currentSize, newSize);
            if (callback != null) {
                callback.onReallocate(address, currentSize, newAddress, newSize);
            }

            return newAddress;
        }

        @Override
        public void free(long address, long size) {
            delegate.free(address, size);
            if (callback != null) {
                callback.onFree(address, size);
            }
        }

        @Override
        public void dispose() {
            delegate.dispose();
            if (callback != null) {
                callback.onDispose();
            }
        }
    }

    interface MemoryAllocatorEventCallback {

        void onAllocate(long addr, long size);

        void onReallocate(long address, long currentSize, long newAddress, long newSize);

        void onFree(long address, long size);

        void onDispose();
    }

    class VersionedLong {

        private final List<Long> versions;

        VersionedLong() {
            versions = new ArrayList<>();
        }

        void set(long v) {
            versions.add(v);
        }

        long get() {
            if (versions.isEmpty()) {
                throw new NoSuchElementException();
            }
            return get(0);
        }

        long get(int prevVersion) {
            return versions.get(versions.size() - 1 - prevVersion);
        }

        List<Long> versions() {
            return versions;
        }

        boolean contains(long v) {
            return versions.contains(v);
        }

        boolean hasUpdates() {
            return !versions.isEmpty();
        }

        void clear() {
            versions.clear();
        }
    }

    static int nextInt(int bound) {
        return ThreadLocalRandom.current().nextInt(bound);
    }

    static boolean nextBoolean() {
        return ThreadLocalRandom.current().nextBoolean();
    }

    long allocateZeroed(int size) {
        long address = keyAllocator.allocate(size);
        AMEM.setMemory(address, size, (byte) 0);
        return address;
    }

    final class AllocatorCallback implements MemoryAllocatorEventCallback {

        private final VersionedLong allocateAddr;
        private final VersionedLong freeAddr;
        private final VersionedLong size;

        AllocatorCallback(VersionedLong allocateAddr, VersionedLong freeAddr, VersionedLong size) {
            this.allocateAddr = allocateAddr;
            this.freeAddr = freeAddr;
            this.size = size;
        }

        @Override
        public void onAllocate(long addr, long sz) {
            allocateAddr.set(addr);
            size.set(sz);
        }

        @Override
        public void onReallocate(long address, long currentSize, long newAddress, long newSize) {
            // no-op
        }

        @Override
        public void onFree(long addr, long sz) {
            freeAddr.set(addr);
            size.set(sz);
        }

        @Override
        public void onDispose() {
            // no-op
        }

        void clear() {
            allocateAddr.clear();
            freeAddr.clear();
            size.clear();
        }
    }

    void insertKeys(int count) {
        insertKeysWithStrategy(count, new DefaultNodeSplitStrategy());
    }

    void insertKeysCompact(int count) {
        insertKeysWithStrategy(count, new EmptyNewNodeSplitStrategy());
    }

    void insertKeysWithStrategy(int count, NodeSplitStrategy nodeSplitStrategy) {
        btree.setNodeSplitStrategy(nodeSplitStrategy);
        for (int i = 0; i < count; ++i) {
            Integer indexKey = i;
            String mapKey = "Name_" + i;
            String value = "Value_" + i;
            NativeMemoryData mapKeyData = toNativeData(mapKey);
            NativeMemoryData valueData = toNativeData(value);
            btree.insert(indexKey, mapKeyData, valueData);
        }
        btree.setNodeSplitStrategy(new DefaultNodeSplitStrategy());
    }

    NativeMemoryData toNativeData(Object value) {
        return ess.toData(value, NATIVE);
    }

    void insertKey(int index) {
        insertKey(index, index);
    }

    void insertKey(int index, int mapKeyIndex) {
        Integer indexKey = index;
        String mapKey = "Name_" + mapKeyIndex;
        String value = "Value_" + index;
        NativeMemoryData mapKeyData = toNativeData(mapKey);
        NativeMemoryData valueData = toNativeData(value);
        btree.insert(indexKey, mapKeyData, valueData);
    }

    NativeMemoryData removeKey(int index) {
        NativeMemoryData data = toNativeData("Name_" + index);
        return btree.remove(index, data);
    }

    int queryKeysCount() {
        Iterator it = btree.lookup(null, true, null, true);
        int count = 0;
        while (it.hasNext()) {
            it.next();
            ++count;
        }
        return count;
    }

    List<QueryableEntry> queryAllKeys() {
        List<QueryableEntry> keys = new ArrayList<>(10000);
        Iterator<QueryableEntry> it = btree.lookup(null, true, null, true);
        while (it.hasNext()) {
            keys.add(it.next());
        }
        return keys;
    }

    void assertOnLeafNodes(long leafAddr, Consumer<Long> action) {
        assertOnLeafNodes(leafAddr, false, action);
    }

    void assertOnLeafNodes(long leafAddr, boolean descending, Consumer<Long> action) {
        assertTrue(leafAddr != NULL_ADDRESS);
        long nodeAddr = leafAddr;
        do {
            action.accept(nodeAddr);
            nodeAddr = descending ? leafNodeAccessor.getBackNode(nodeAddr) : leafNodeAccessor.getForwNode(nodeAddr);
        } while (nodeAddr != NULL_ADDRESS);
    }

    static class MockedLockManager implements LockManager {

        MockedLockManager() {
            // no-op
        }

        @Override
        public void readLock(long lockAddr) {
            // no-op
        }

        @Override
        public void writeLock(long lockAddr) {
            // no-op
        }

        @Override
        public boolean tryUpgradeToWriteLock(long lockAddr) {
            return true;
        }

        @Override
        public boolean tryWriteLock(long lockAddr) {
            return true;
        }

        @Override
        public void instantDurationWriteLock(long lockAddr) {
            // no-op
        }

        @Override
        public void releaseLock(long lockAddr) {
            // no-op
        }
    }

    void assertNodesState(long nodeAddr, Consumer<Long> test) {
        int level = getNodeLevel(nodeAddr);
        test.accept(nodeAddr);
        if (level == 0) {
            return;
        } else {
            for (int i = 0; i <= getKeysCount(nodeAddr); ++i) {
                long childAddr = innerNodeAccessor.getValueAddr(nodeAddr, i);
                assertNodesState(childAddr, test);
            }
        }
    }

    void assertNoLocksLeft() {
        assertNodesState(rootAddr, nodeAddr -> assertNoLocksLeft(nodeAddr));
    }

    void assertNoLocksLeft(long nodeAddr) {
        long lockState = getLockState(nodeAddr);
        int nodeLevel = getNodeLevel(nodeAddr);
        int depth = getNodeLevel(rootAddr);
        assertEquals("Addr " + nodeAddr + ", level " + nodeLevel + ", depth " + depth, 0, getUsersCount(lockState));
        assertEquals("Addr " + nodeAddr + ", level " + nodeLevel + ", depth " + depth, 0, getReadWaitersCount(lockState));
        assertEquals("Addr " + nodeAddr + ", level " + nodeLevel + ", depth " + depth, 0, getWriteWaitersCount(lockState));
        assertEquals("Addr " + nodeAddr + ", level " + nodeLevel + ", depth " + depth, 0, lockState);
    }

    void assertNoLocksLeft(GlobalIndexPoolingAllocator indexAllocator) {
        for (Long nodeAddr : indexAllocator.consumeNodeAddressesFromQueue()) {
            assertEquals(0, getLockState(nodeAddr));
        }
    }

    void assertNoKeys(GlobalIndexPoolingAllocator indexAllocator) {
        for (Long nodeAddr : indexAllocator.consumeNodeAddressesFromQueue()) {
            assertEquals(0, getKeysCount(nodeAddr));
        }
    }
}
