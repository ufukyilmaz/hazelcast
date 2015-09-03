package com.hazelcast.elasticmemory;

import com.hazelcast.memory.NativeOutOfMemoryError;

class IntegerQueue {

    private static final int NULL_VALUE = -1;

    private final int maxSize;
    private final int[] array;

    private int add;
    private int remove;
    private int size;

    public IntegerQueue(int maxSize) {
        this.maxSize = maxSize;
        this.array = new int[maxSize];
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
            throw new NativeOutOfMemoryError("Queue has " + size + " available chunks. Data requires " + count + " chunks."
                    + " Storage is full!");
        }

        for (int i = 0; i < count; i++) {
            indexes[i] = poll();
        }
        return indexes;
    }
}
