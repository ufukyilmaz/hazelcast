package com.hazelcast.elastic.offheapstorage;

import com.hazelcast.elastic.offheapstorage.iterator.OffHeapKeyIterator;
import com.hazelcast.elastic.offheapstorage.iterator.value.OffHeapValueIterator;

/***
 * Represents abstract API for the OffHeap append-only key value storage
 */
public interface OffHeapKeyValueStorage {
    /***
     * Return first value entry address for the corresponding key entry
     *
     * @param keyEntryAddress - key entry address
     * @return address of the first value entry in the chain of values
     */
    long getValueEntryAddress(long keyEntryAddress);

    /***
     * @param valueEntryAddress - value entry address
     * @return address of the next value address
     * 0 - if there is no next address
     */
    long getNextValueEntryAddress(long valueEntryAddress);

    /***
     * @param valueEntryAddress - value entry address
     * @return address of the value's content
     */
    long getValueAddress(long valueEntryAddress);

    /***
     * @param valueEntryAddress - value entry address
     * @return actually written bytes to the value's buffer
     */
    long getValueWrittenBytes(long valueEntryAddress);

    /***
     * @param valueEntryAddress - value entry address
     * @return actually allocated bytes to the value's buffer
     */
    long getValueAllocatedBytes(long valueEntryAddress);

    /***
     * @return iterator over keys of the storage
     */
    OffHeapKeyIterator keyIterator();

    /**
     * @param keyEntryPointer - address of the corresponding keyEntry
     * @return iterator over values of the corresponding key
     */
    OffHeapValueIterator valueIterator(long keyEntryPointer);

    /***
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

    /***
     * Looking for key equal to buffer by address;
     * keyAddress with params keyWrittenBytes and keyAllocatedBytes;
     * <p/>
     * In case if not-found - if createIfNotExists==true - create new key entry;
     * if createIfNotExists==false - don't create new key entry - return 0L;
     *
     * @param keyAddress        - address of the key
     * @param keyWrittenBytes   - amount of written bytes of the key
     * @param keyAllocatedBytes - amount of allocated bytes of the key
     * @param createIfNotExists - flag which determines if we want to create entry if it doesn't exist
     * @return address of the key entry
     */
    long getKeyEntry(long keyAddress, long keyWrittenBytes, long keyAllocatedBytes, boolean createIfNotExists);

    /***
     * Looking for key equal to buffer by address;
     * keyAddress with params keyWrittenBytes and keyAllocatedBytes;
     * <p/>
     * In case if not-found - if createIfNotExists==true - create new key entry;
     * if createIfNotExists==false - don't create new key entry - return 0L;
     *
     * @param keyAddress        - address of the key
     * @param keyWrittenBytes   - amount of written bytes of the key
     * @param keyAllocatedBytes - amount of allocated bytes of the key
     * @param comparator        - comparator to be used, if null default passed in constructor will be used
     * @param createIfNotExists - flag which determines if we want to create entry if it doesn't exist
     * @return address of the key entry
     */
    long getKeyEntry(long keyAddress, long keyWrittenBytes, long keyAllocatedBytes, OffHeapComparator comparator, boolean createIfNotExists);

    /***
     * @param keyEntryPointer - address of the key entry
     * @return address of the key buffer in corresponding key entry
     */
    long getKeyAddress(long keyEntryPointer);

    /***
     * @param keyEntryPointer - address of the key entry
     * @return amount of written bytes of the key
     */
    long getKeyWrittenBytes(long keyEntryPointer);

    /***
     * @param keyEntryPointer - address of the key entry
     * @return amount of allocated bytes of the key
     */
    long getKeyAllocatedBytes(long keyEntryPointer);

    /***
     * Marks key with specified keyEntry address with value marker.
     *
     * @param keyEntryPointer - address of the keyEntry
     * @param marker          - marker value
     */
    void markKeyEntry(long keyEntryPointer, byte marker);

    /***
     * Return marker's value for the specified keyEntry
     *
     * @param keyEntryPointer - address of the keyEntry
     * @return marker's value
     */
    byte getKeyEntryMarker(long keyEntryPointer);

    /***
     * @return amount of elements in the tree
     */
    long count();

    /***
     * Validate if storage is in consistent state
     *
     * @return true - in case of storage is in consistent state
     * false - in case of storage is not in consistent state
     */
    boolean validate();

    /***
     * Release of memory (of all keys,values and entries) associated with storage
     */
    void dispose();

    /***
     * Release just memory associated with tree (doesn't release keys and values)
     */
    void disposeEntries();
}