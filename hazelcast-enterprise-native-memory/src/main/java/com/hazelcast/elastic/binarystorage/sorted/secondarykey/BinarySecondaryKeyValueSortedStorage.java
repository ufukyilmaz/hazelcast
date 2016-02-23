package com.hazelcast.elastic.binarystorage.sorted.secondarykey;

import com.hazelcast.elastic.binarystorage.BinaryComparator;
import com.hazelcast.elastic.binarystorage.iterator.secondarykey.BinarySecondaryKeyIterator;
import com.hazelcast.elastic.binarystorage.sorted.BinaryKeyValueSortedStorage;
import com.hazelcast.elastic.binarystorage.sorted.OrderingDirection;

/**
 * Provides key-value storage functionality with 2-layer keys structure
 * <pre>
 *          KEY1 -----
 *                 SECONDARY_KEY1_1
 *                     value1
 *                     value2
 *                     value3
 *                 SECONDARY_KEY1_2
 *                     value4
 *                     value5
 *                     value6
 *                 SECONDARY_KEY1_3
 *                     value7
 *                     value8
 *                     value9
 *          KEY2 -----
 *                 SECONDARY_KEY2_1
 *                     value1
 *                     value2
 *                     value3
 *                 SECONDARY_KEY2_2
 *                     value4
 *                     value5
 *                     value6
 *                 SECONDARY_KEY2_3
 *                     value7
 *                     value8
 *                     value9
 * </pre>
 * <p/>
 * Keys are sorted on 1-st level and on each branch of the 2-nd level.
 * RedBlack structure is used in both keys levels.
 */
public interface BinarySecondaryKeyValueSortedStorage extends BinaryKeyValueSortedStorage {

    /**
     * Looking for key equal to buffer by address;
     * keyAddress with params keyWrittenBytes and  keyAllocatedBytes;
     * In case if found - append value represented by;
     * valueAddress with params  valueWrittenBytes and  valueAllocatedBytes to the chain of key's values;
     * <p/>
     * In case if not-found - adds new key to the corresponding place in the tree and assign value to this 'new' key;
     *
     * @param keyAddress                 - address of the key;
     * @param keyWrittenBytes            - amount of written bytes of the key;
     * @param keyAllocatedBytes          - amount of allocated bytes of the key;
     * @param secondaryKeyAddress        - address of the secondaryKey;
     * @param secondaryKeyWrittenBytes   - amount of written bytes of the secondary key;
     * @param secondaryKeyAllocatedBytes - amount of allocated bytes of the secondary key;
     * @return address of the key entry;
     */
    long put(long keyAddress, long keyWrittenBytes, long keyAllocatedBytes,
             long secondaryKeyAddress, long secondaryKeyWrittenBytes, long secondaryKeyAllocatedBytes);

    /**
     * Looking for key equal to buffer by address;
     * keyAddress with params keyWrittenBytes and  keyAllocatedBytes;
     * In case if found - append value represented by;
     * valueAddress with params  valueWrittenBytes and  valueAllocatedBytes to the chain of key's values;
     * <p/>
     * In case if not-found - adds new key to the corresponding place in the tree and assign value to this 'new' key;
     *
     * @param keyAddress                 - address of the key;
     * @param keyWrittenBytes            - amount of written bytes of the key;
     * @param keyAllocatedBytes          - amount of allocated bytes of the key;
     * @param secondaryKeyAddress        - address of the secondaryKey;
     * @param secondaryKeyWrittenBytes   - amount of written bytes of the secondary key;
     * @param secondaryKeyAllocatedBytes - amount of allocated bytes of the secondary key;
     * @param comparator                 - comparator to be used, if null - default storage's comparator to be used;
     * @return address of the key entry;
     */
    long put(long keyAddress, long keyWrittenBytes, long keyAllocatedBytes,
             long secondaryKeyAddress, long secondaryKeyWrittenBytes, long secondaryKeyAllocatedBytes,
             BinaryComparator comparator);

    /**
     * Looking for key equal to buffer by address;
     * keyAddress with params keyWrittenBytes and  keyAllocatedBytes;
     * In case if found - append value represented by;
     * valueAddress with params  valueWrittenBytes and  valueAllocatedBytes to the chain of key's values;
     * <p/>
     * Perform the same for the secondaryKey address and bind new value to the secondary key
     * <p/>
     * In case if not-found - adds new key to the corresponding place in the tree and assign value to this 'new' key;
     *
     * @param keyAddress                 - address of the key;
     * @param keyWrittenBytes            - amount of written bytes of the key;
     * @param keyAllocatedBytes          - amount of allocated bytes of the key;
     * @param secondaryKeyAddress        - address of the secondaryKey;
     * @param secondaryKeyWrittenBytes   - amount of written bytes of the secondary key;
     * @param secondaryKeyAllocatedBytes - amount of allocated bytes of the secondary key;
     * @param valueAddress               - address of the value;
     * @param valueWrittenBytes          - amount of written bytes of the value;
     * @param valueAllocatedBytes        - amount of allocated bytes of the value;
     * @return address of the key entry;
     */
    @SuppressWarnings("checkstyle:parameternumber")
    long put(long keyAddress, long keyWrittenBytes, long keyAllocatedBytes,
             long secondaryKeyAddress, long secondaryKeyWrittenBytes, long secondaryKeyAllocatedBytes,
             long valueAddress, long valueWrittenBytes, long valueAllocatedBytes
    );

    /**
     * Looking for key equal to buffer by address;
     * keyAddress with params keyWrittenBytes and  keyAllocatedBytes;
     * In case if found - append value represented by;
     * valueAddress with params  valueWrittenBytes and  valueAllocatedBytes to the chain of key's values;
     * <p/>
     * Perform the same for the secondaryKey address and bind new value to the secondary key
     * <p/>
     * In case if not-found - adds new key to the corresponding place in the tree and assign value to this 'new' key;
     *
     * @param keyAddress                 - address of the key;
     * @param keyWrittenBytes            - amount of written bytes of the key;
     * @param keyAllocatedBytes          - amount of allocated bytes of the key;
     * @param secondaryKeyAddress        - address of the secondaryKey;
     * @param secondaryKeyWrittenBytes   - amount of written bytes of the secondary key;
     * @param secondaryKeyAllocatedBytes - amount of allocated bytes of the secondary key;
     * @param valueAddress               - address of the value;
     * @param valueWrittenBytes          - amount of written bytes of the value;
     * @param valueAllocatedBytes        - amount of allocated bytes of the value;
     * @param primaryKeyComparator       - comparator to be used for the primary keys,
     *                                   if null - default storage's comparator to be used;
     * @param secondaryKeyComparator     - comparator to be used for the secondary keys,
     *                                   if null - default storage's comparator to be used;
     * @return address of the key entry;
     */
    @SuppressWarnings("checkstyle:parameternumber")
    long put(long keyAddress, long keyWrittenBytes, long keyAllocatedBytes,
             long secondaryKeyAddress, long secondaryKeyWrittenBytes, long secondaryKeyAllocatedBytes,
             long valueAddress, long valueWrittenBytes, long valueAllocatedBytes,
             BinaryComparator primaryKeyComparator, BinaryComparator secondaryKeyComparator
    );

    /**
     * @param keyEntryPointer   - address of the key entry
     * @param orderingDirection - direction of the sorting
     * @return secondary key iterator for the specified key
     */
    BinarySecondaryKeyIterator secondaryKeyIterator(long keyEntryPointer, OrderingDirection orderingDirection);

    /**
     * @param direction -
     *                  ASC - 1
     *                  DESC - 0
     * @return address of the first key entry in accordance with direction
     */
    long firstSecondaryKeyEntry(OrderingDirection direction, long keyEntryAddress);

    /**
     * @return address of the last inserted secondary key entry
     */
    long getLastInsertedSecondaryKeyEntry();

    /**
     * @param direction         -
     *                          ASC - 1
     *                          DESC - 0
     * @param keyEntryAddress   - address of the corresponding key entry
     * @param secondaryKeyEntry - secondary keys entry address to start from
     * @return address of the first key entry in accordance with direction
     */
    long firstSecondaryKeyEntry(OrderingDirection direction, long keyEntryAddress, long secondaryKeyEntry);
}
