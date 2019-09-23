package com.hazelcast.internal.elastic.tree;

import com.hazelcast.internal.memory.MemoryBlock;

import java.util.Iterator;

/**
 * API to interact with a Tree entry, to access the Key and/or Values associated with it.
 */
public interface OffHeapTreeEntry {

    //TODO tkountis - This could be an Entry factory, to allow support for other types,
    // regardless of the stored blob. Same for value!
    MemoryBlock getKey();

    boolean hasValues();

    Iterator<MemoryBlock> values();

}
