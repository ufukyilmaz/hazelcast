package com.hazelcast.internal.bplustree;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.internal.elastic.tree.MapEntryFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.MAX_NODE_SIZE;
import static com.hazelcast.internal.bplustree.HDBTreeNodeBaseAccessor.MIN_NODE_SIZE;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BPlusTreeLimitsTest extends BPlusTreeTestSupport {

    @Override
    int getNodeSize() {
        return 128;
    }

    @Test
    public void testOverflowBTreeLevel() {

        assertThrows(BPlusTreeLimitException.class, () -> {
            for (int n = 0; n <= 768; ++n) {
                insertKey(n);
            }
        });
    }

    @Test
    public void testOverflowNodeSize() {
        BPlusTreeKeyComparator keyComparator = new DefaultBPlusTreeKeyComparator(ess);
        BPlusTreeKeyAccessor keyAccessor = new DefaultBPlusTreeKeyAccessor(ess);
        LockManager lockManager = newLockManager();
        MapEntryFactory factory = new OnHeapEntryFactory(ess, null);
        assertThrows(IllegalArgumentException.class, () -> {
            btree = HDBPlusTree.newHDBTree(ess, keyAllocator, delegatingIndexAllocator, lockManager,
                    keyComparator, keyAccessor, factory, MAX_NODE_SIZE + 1);
        });

        assertThrows(IllegalArgumentException.class, () -> {
            btree = HDBPlusTree.newHDBTree(ess, keyAllocator, delegatingIndexAllocator, lockManager,
                    keyComparator, keyAccessor, factory, MIN_NODE_SIZE / 2);
        });

    }

}
