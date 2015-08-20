package com.hazelcast.elastic.queue;

import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;

import java.util.AbstractQueue;
import java.util.Iterator;

/**
 * @author mdogan 22/01/14
 */
public final class ConcurrentLinkedElasticQueue<E> extends AbstractQueue<E> implements ElasticQueue<E> {

    private final LongConcurrentLinkedQueue queue;
    private final EnterpriseSerializationService ss;

    public ConcurrentLinkedElasticQueue(EnterpriseSerializationService ss, MemoryAllocator malloc) {
        this.ss = ss;
        queue = new LongConcurrentLinkedQueue(malloc, 0L);
    }

    @Override
    public boolean offer(final E e) {
        NativeMemoryData binary = ss.toData(e, DataType.NATIVE);
        if (queue.offer(binary.address())) {
            return true;
        }
        ss.disposeData(binary);
        return false;
    }

    @Override
    public E poll() {
        return first(true);
    }

    @Override
    public E peek() {
        return first(false);
    }

    private E first(boolean remove) {
        long address = queue.poll();
        return toItem(address, remove);
    }

    private E toItem(long address, boolean dispose) {
        if (address != queue.nullItem()) {
            NativeMemoryData data = new NativeMemoryData().reset(address);
            try {
                return ss.toObject(data);
            } finally {
                if (dispose) {
                    ss.disposeData(data);
                }
            }
        }
        return null;
    }

    @Override
    public int size() {
        return queue.size();
    }

    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    @Override
    public void clear() {
        queue.clear();
    }

    @Override
    public void destroy() {
        queue.destroy();
    }

    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException();
    }
}

