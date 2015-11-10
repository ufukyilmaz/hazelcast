package com.hazelcast.elastic.offheapstorage.iterator.value;

/***
 * Represents API to iterate over key's entry values of the off-heap storage structure
 */
public interface OffHeapValueIterator {
    /***
     * @return true if there are also elements to fetch, false in opposite
     */
    boolean hasNext();

    /***
     * @return next pointer to value entry
     */
    long next();

    /***
     * Set up key entry address to iterate over entries
     *
     * @param keyEntryPointer - address of the key entry
     */
    void reset(long keyEntryPointer);
}
