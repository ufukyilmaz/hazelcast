package com.hazelcast.elasticmemory;

import com.hazelcast.elasticmemory.error.OffHeapOutOfMemoryError;

/**
* @author mdogan 10/10/13
*/
class IntegerQueue {
    private final static int NULL_VALUE = -1;

    private final int maxSize;
    private final int[] array;
    private int add = 0;
    private int remove = 0;
    private int size = 0;

    public IntegerQueue(int maxSize) {
        this.maxSize = maxSize;
        array = new int[maxSize];
    }

    public boolean offer(int value) {
        if (size == maxSize) {
            return false;
        }
        array[add++] = value;
        size++;
        if (add == maxSize) {
            add = 0;
        }
        return true;
    }

    public int poll() {
        if (size == 0) {
            return NULL_VALUE;
        }
        final int value = array[remove];
        array[remove++] = NULL_VALUE;
        size--;
        if (remove == maxSize) {
            remove = 0;
        }
        return value;
    }

    public int[] poll(final int[] indexes) {
        final int count = indexes.length;
        if (count > size) {
            throw new OffHeapOutOfMemoryError("Segment has " + size + " available chunks. " +
                    "Data requires " + count + " chunks. Segment is full!");
        }

        for (int i = 0; i < count; i++) {
            indexes[i] = poll();
        }
        return indexes;
    }
}
