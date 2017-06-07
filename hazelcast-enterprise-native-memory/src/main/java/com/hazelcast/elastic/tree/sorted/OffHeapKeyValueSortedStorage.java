package com.hazelcast.elastic.tree.sorted;

import com.hazelcast.elastic.tree.OffHeapComparator;
import com.hazelcast.elastic.tree.OffHeapKeyValueStorage;
import com.hazelcast.elastic.tree.iterator.OffHeapKeyIterator;

/**
 * Represents API for sorted off-heap key-value storage
 */
public interface OffHeapKeyValueSortedStorage extends OffHeapKeyValueStorage {

    /**
     * @param direction -
     *                  ASC - 1;
     *                  DESC - 0;
     * @return address of the first key entry in accordance with direction;
     */
    long first(OrderingDirection direction);

    /**
     * @param direction -
     *                  ASC - 1
     *                  DESC - 0
     * @param keyEntry  - keys entry address to start from
     * @return address of the first key entry in accordance with direction
     */
    long first(OrderingDirection direction,
               long keyEntry);

    /**
     * @param pointer   - keys entry address to start from
     * @param direction -
     *                  ASC - 1
     *                  DESC - 0
     * @return next key entry address in accordance with direction
     */
    long getNext(long pointer,
                 OrderingDirection direction);

    /**
     * @param direction ASC - 1
     *                  DESC - 0
     * @return iterator with iterating direction corresponding to direction
     */
    OffHeapKeyIterator keyIterator(OrderingDirection direction);

    /**
     * @param direction       ASC - 1
     *                        DESC - 0
     * @param keyEntryAddress keyEntryAddress to begin the iteration with
     * @return iterator with iterating direction corresponding to direction
     */
    OffHeapKeyIterator keyIterator(OrderingDirection direction, long keyEntryAddress);


    /**
     * Default ASC iterator.
     *
     * @param keyEntryAddress keyEntryAddress to begin the iteration with
     * @return iterator with iterating direction corresponding to direction
     */
    OffHeapKeyIterator keyIterator(long keyEntryAddress);

    /**
     * Looking for key equal to buffer by address;
     * keyAddress with params keyWrittenBytes and  keyAllocatedBytes;
     * In case if found - append value represented by;
     * valueAddress with params  valueWrittenBytes and  valueAllocatedBytes to the chain of key's values;
     * <p/>
     * In case if not-found - adds new key to the corresponding place in the tree and assign value to this 'new' key;
     *
     * @param keyAddress          - address of the key;
     * @param keyWrittenBytes     - amount of written bytes of the key;
     * @param keyAllocatedBytes   - amount of allocated bytes of the key;
     * @param valueAddress        - address of the value;
     * @param valueWrittenBytes   - amount of written bytes of the value;
     * @param valueAllocatedBytes - amount of allocated bytes of the value;
     * @return address of the key entry;
     */
    long put(long keyAddress, long keyWrittenBytes, long keyAllocatedBytes,
             long valueAddress, long valueWrittenBytes, long valueAllocatedBytes);

    /**
     * Looking for key equal to buffer by address;
     * keyAddress with params keyWrittenBytes and  keyAllocatedBytes;
     * In case if found - append value represented by;
     * valueAddress with params  valueWrittenBytes and  valueAllocatedBytes to the chain of key's values;
     * <p/>
     * In case if not-found - adds new key to the corresponding place in the tree and assign value to this 'new' key;
     *
     * @param keyAddress          - address of the key;
     * @param keyWrittenBytes     - amount of written bytes of the key;
     * @param keyAllocatedBytes   - amount of allocated bytes of the key;
     * @param valueAddress        - address of the value;
     * @param valueWrittenBytes   - amount of written bytes of the value;
     * @param valueAllocatedBytes - amount of allocated bytes of the value;
     * @param comparator          - comparator to be used, if null default comparator passed from constructor to be used;
     * @return address of the key entry;
     */
    long put(long keyAddress, long keyWrittenBytes, long keyAllocatedBytes,
             long valueAddress, long valueWrittenBytes, long valueAllocatedBytes,
             OffHeapComparator comparator);

}
