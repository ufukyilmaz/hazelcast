package com.hazelcast.elastic.tree;

import com.hazelcast.elastic.tree.impl.RedBlackTreeStore;
import com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.memory.MemoryBlock;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.PoolingMemoryManager;
import com.hazelcast.nio.Bits;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Random;

@RunWith(HazelcastSerialClassRunner.class)
@Category({NightlyTest.class, ParallelJVMTest.class})
public class OffHeapTreeStoreSlowTest extends OffHeapTreeTestSupport {

    @Test
    public void brute_force_add_remove() {
        MemoryAllocator malloc = new PoolingMemoryManager(MemorySize.parse("2G"));
        RedBlackTreeStore storage = new RedBlackTreeStore(
                malloc, new OffHeapTreeStoreTest.DataComparator(GlobalMemoryAccessorRegistry.MEM), true);

        int cnt = 4000000; // 4M
        HashMap<Integer, MemoryBlock> entries = new HashMap<Integer, MemoryBlock>();
        Random random = new Random();

        for (int i = 0; i < cnt; i++) {
            long address = malloc.allocate(Bits.INT_SIZE_IN_BYTES);

            MemoryBlock blob = new MemoryBlock(address, Bits.INT_SIZE_IN_BYTES);
            int r = random.nextInt(cnt * 100);
            blob.writeInt(0, r);

            storage.put(blob, blob);
            entries.put(r, blob);

            if (i % 100000 == 0) {
                System.out.println(i + "...");
            }
        }

        System.out.println("Finished adding...");

        int count = 0;
        for (Integer r : entries.keySet()) {
            MemoryBlock block = entries.get(r);
            OffHeapTreeEntry entry = storage.getEntry(block);
            storage.remove(entry);

            if (++count % 100000 == 0) {
                System.out.println(count + "...");
            }
        }

        storage.dispose(false);
    }

}
