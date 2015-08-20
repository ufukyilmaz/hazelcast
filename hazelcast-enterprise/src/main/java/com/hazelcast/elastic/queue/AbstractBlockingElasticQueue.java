package com.hazelcast.elastic.queue;

import com.hazelcast.elastic.LongIterator;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.nio.serialization.DataType;
import com.hazelcast.nio.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * @author mdogan 22/01/14
 */
abstract class AbstractBlockingElasticQueue<E> extends AbstractQueue<E> implements BlockingElasticQueue<E> {

    private final LongBlockingQueue queue;
    private final EnterpriseSerializationService ss;

    protected AbstractBlockingElasticQueue(EnterpriseSerializationService ss, int capacity, MemoryAllocator malloc) {
        this.ss = ss;
        queue = createQueue(malloc, capacity, 0L);
    }

    protected abstract LongBlockingQueue createQueue(MemoryAllocator malloc, int capacity, long nullItem);

    @Override
    public final boolean offer(final E e) {
        NativeMemoryData binary = ss.toData(e, DataType.NATIVE);
        if (queue.offer(binary.address())) {
            return true;
        }
        ss.disposeData(binary);
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
    public final void put(final E e) throws InterruptedException {
        NativeMemoryData binary = ss.toData(e, DataType.NATIVE);
        queue.put(binary.address());
    }

    @Override
    public final boolean offer(final E e, final long timeout, final TimeUnit unit) throws InterruptedException {
        NativeMemoryData binary = ss.toData(e, DataType.NATIVE);
        if (queue.offer(binary.address(), timeout, unit)) {
            return true;
        }
        ss.disposeData(binary);
        return false;
    }

    @Override
    public final E take() throws InterruptedException {
        long address = queue.take();
        return toItem(address, true);
    }

    @Override
    public final E poll(final long timeout, final TimeUnit unit) throws InterruptedException {
        long address = queue.poll(timeout, unit);
        return toItem(address, true);
    }

    @Override
    public final int remainingCapacity() {
        return queue.remainingCapacity();
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
    }

    @Override
    public final int drainTo(final Collection<? super E> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public final int drainTo(final Collection<? super E> c, final int maxElements) {
        ConsumerImpl consumer = new ConsumerImpl(c, maxElements);
        queue.consume(consumer);
        return maxElements - consumer.count;
    }

    private class ConsumerImpl implements LongConsumer {
        final Collection<? super E> coll;
        int count;

        ConsumerImpl(final Collection<? super E> coll, final int count) {
            this.coll = coll;
            this.count = count;
        }

        public boolean consume(final long value) {
            if (count > 0) {
                NativeMemoryData data = new NativeMemoryData().reset(value);
                E item = ss.toObject(data);
                if (coll.add(item)) {
                    ss.disposeData(data);
                    count--;
                    return true;
                }
            }
            return false;
        }
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
