package com.hazelcast.internal.elastic.tree;

import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.memory.MemoryBlock;

import java.util.Iterator;

/***
 * Represents abstract API for the OffHeap append-only key value storage
 */
public interface OffHeapTreeStore
        extends Iterable<OffHeapTreeEntry> {

    /***
     * @return iterate over keys of the store from root in ascending order
     */
    Iterator<OffHeapTreeEntry> entries();

    /***
     * @return iterate over keys of the store from root with the given direction
     */
    Iterator<OffHeapTreeEntry> entries(OrderingDirection direction);

    /***
     * @return iterate over keys of the store from entry
     */
    Iterator<OffHeapTreeEntry> entries(OffHeapTreeEntry entry);

    /***
     * @return iterate over keys of the store from the entry with the given direction
     */
    Iterator<OffHeapTreeEntry> entries(OffHeapTreeEntry entry, OrderingDirection direction);

    /***
     * Looking for key equal to blob;
     * In case if found - append value represented by value to the chain of key's values;
     * <p/>
     * In case if not-found - adds new key to the corresponding place in the tree and assign value to this 'new' key;
     *
     * @param key           - The key blob;
     * @param value         - The value blob;
     *
     * @return The Entry associated with the key;
     */
    OffHeapTreeEntry put(MemoryBlock key, MemoryBlock value);

    /***
     * Looking for key equal to blob;
     *
     * @param key           - The key blob;
     * @return The Entry associated with the key or <code>null</code> if not found
     */
    OffHeapTreeEntry getEntry(HeapData key);

    /***
     * Looking for key equal to blob;
     *
     * @param key           - The key blob;
     * @return The Entry associated with the key or <code>null</code> if not found
     */
    OffHeapTreeEntry getEntry(MemoryBlock key);

    /***
     * Looking for key equal to blob using the given comparator;
     * In case if not-found - if createIfNotExists==true - create new key entry;
     * if createIfNotExists==false - don't create new key entry - return null;
     *
     * @param key               - The key blob;
     * @param comparator        - comparator to be used, if null default passed in constructor will be used
     * @return address of the key entry
     */
    OffHeapTreeEntry getEntry(MemoryBlock key,  OffHeapComparator comparator);

    /***
     * Looking for key equal or closest to blob;
     * <p/>
     *
     * @param key               - The key blob;
     * @return address of the key entry equal od closest to, or zero address if map empty
     */
    OffHeapTreeEntry searchEntry(MemoryBlock key);

    /***
     * Release of memory (of all keys,values and entries) associated with storage
     *
     * **WARNING** Special attention is required when disposing a tree this way, if NOT empty.
     * When a tree contains duplicate values, either on the same entry or different ones, this can cause the VM
     * to crash when free'ing the same address twice. The safe way is to always dispose the entries one by one, and the
     * key/values should be disposed by their original owner.
     */
    void dispose(boolean releasePayloads);

    /***
     * Removes the entry from the tree and disposes used space associated with that entry
     * including Key and Value blobs.
     *
     * All resources associated with the entry will be disposed, however the actual Entry (Key & Value(s))
     * are assumed to be managed by their owners, therefore no attempt is made to dispose them.
     * To release their resources, first fetch them with {@link #getEntry(MemoryBlock)} and dispose the allocated chunks on
     * the addresses of the returned Entry Key & Values. Eg.
     *
     * <code>
     * <pre>
     * OffHeapTreeEntry entry = getEntry(...);
     * dispose(entry.getKey());
     * for (MemoryBlock value : entry) {
     *     dispose(value);
     * }
     * remove(entry);
     * </pre>
     * </code>
     * @param entry The tree entry to be removed
     */
    void remove(OffHeapTreeEntry entry);

}
