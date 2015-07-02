package com.hazelcast.elastic.queue;

import com.hazelcast.elastic.LongIterator;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.memory.MemoryManager;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.nio.serialization.impl.NativeMemoryData;

import java.util.AbstractQueue;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

/**
 * @author mdogan 22/01/14
 */
abstract class AbstractElasticQueue<E> extends AbstractQueue<E> implements ElasticQueue<E> {

    private final LongQueue queue;
    private final EnterpriseSerializationService ss;
    private final Queue<NativeMemoryData> localBinaryQ = new ArrayDeque<NativeMemoryData>(1024);

    public AbstractElasticQueue(EnterpriseSerializationService ss, int capacity, MemoryAllocator malloc) {
        this.ss = ss;
        queue = createQueue(malloc, capacity, 0L);
    }

    protected abstract LongQueue createQueue(MemoryAllocator malloc, int capacity, long nullItem);

    private void enqueueBinary(NativeMemoryData data) {
        if (data == null) {
            return;
        }
        data.reset(MemoryManager.NULL_ADDRESS);
        localBinaryQ.offer(data);
    }

    private NativeMemoryData addressToBinary(long address) {
        if (address > 0L) {
            NativeMemoryData binary = localBinaryQ.poll();
            if (binary == null) {
                binary = new NativeMemoryData();
            }
            return binary.reset(address);
        }
        return null;
    }


    @Override
    public final boolean offer(final E e) {
        NativeMemoryData binary = ss.toData(e, DataType.NATIVE);
        if (queue.offer(binary.address())) {
            enqueueBinary(binary);
            return true;
        }
        ss.disposeData(binary);
        enqueueBinary(binary);
        return false;
    }

    @Override
    public final E poll() {
        return first(true);
    }

    @Override
    public final E peek() {
        return first(false);
    }

    private E first(boolean remove) {
        long address = queue.poll();
        return toItem(address, remove);
    }

    private E toItem(long address, boolean dispose) {
        if (address != queue.nullItem()) {
            NativeMemoryData binary = addressToBinary(address);
            try {
                return ss.toObject(binary);
            } finally {
                if (dispose) {
                    ss.disposeData(binary);
                }
                enqueueBinary(binary);
            }
        }
        return null;
    }

    @Override
    public final void clear() {
        queue.clear();
    }

    @Override
    public final int size() {
        return queue.size();
    }

    @Override
    public final boolean isEmpty() {
        return queue.isEmpty();
    }

    @Override
    public final void destroy() {
        queue.destroy();
        localBinaryQ.clear();
    }

    @Override
    public final Iterator<E> iterator() {
        return new ItemIterator();
    }

    private class ItemIterator implements Iterator<E> {
        final LongIterator iter = queue.iterator();

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public E next() {
            long address = iter.next();
            return toItem(address, false);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
