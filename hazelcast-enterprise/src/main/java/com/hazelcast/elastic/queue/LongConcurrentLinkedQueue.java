package com.hazelcast.elastic.queue;

import com.hazelcast.elastic.LongIterator;
import com.hazelcast.memory.GarbageCollectable;
import com.hazelcast.memory.MemoryAllocator;
import com.hazelcast.nio.UnsafeHelper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import sun.misc.Unsafe;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * See http://www.cs.rochester.edu/u/scott/papers/1996_PODC_queues.pdf
 * See ABA problem; http://en.wikipedia.org/wiki/ABA_problem
 */
public final class LongConcurrentLinkedQueue implements LongQueue, GarbageCollectable {

    private static final long NULL_PTR = 0L;

    private static final int NODE_SIZE = 16;

    private static final int NEXT_OFFSET = 8;

    private final MemoryAllocator malloc;

    private final long nullItem;

    private final AtomicLong head;

    private final AtomicLong tail;

    private final AtomicLong size = new AtomicLong();

    private final int threadLocalAddressCapacity;

    private final ConcurrentHashMap<Thread, LocalAddressQueue> localAddressQueues
            = new ConcurrentHashMap<Thread, LocalAddressQueue>();

    public LongConcurrentLinkedQueue(MemoryAllocator malloc, long nullValue) {
        this(malloc, nullValue, 1024);
    }

    public LongConcurrentLinkedQueue(MemoryAllocator malloc, long nullValue, int threadLocalAddressCapacity) {
        this.malloc = malloc;
        this.threadLocalAddressCapacity = threadLocalAddressCapacity;
        nullItem = nullValue;

        // allocating directly, to avoid thread local node caching
        long address = malloc.allocate(NODE_SIZE);
        setNode(address, nullItem);
        head = new AtomicLong(address);
        tail = new AtomicLong(head.get());
    }

    private long newNode(final long e) {
        LocalAddressQueue queue = getLocalAddressQueue();
        long address = queue.allocate();
        setNode(address, e);
        return address;
    }

    private void setNode(long address, long e) {
        Unsafe unsafe = UnsafeHelper.UNSAFE;
        unsafe.putLongVolatile(null, address, e);
        unsafe.putLongVolatile(null, address + NEXT_OFFSET, NULL_PTR);
    }

    private long getItem(long node) {
        if (node == NULL_PTR) {
            throw new NullPointerException("Node is null! " + node);
        }
        return UnsafeHelper.UNSAFE.getLongVolatile(null, node);
    }

    private long getNextNode(long node) {
        if (node == NULL_PTR) {
            throw new NullPointerException("Node is null! " + node);
        }
        return UnsafeHelper.UNSAFE.getLongVolatile(null, node + NEXT_OFFSET);
    }

    private boolean casNextNode(long node, long current, long value) {
        if (node == NULL_PTR) {
            throw new NullPointerException("Node is null! " + node);
        }
        return UnsafeHelper.UNSAFE.compareAndSwapLong(null, node + NEXT_OFFSET, current, value);
    }

    public boolean offer(long value) {
        if (value == nullItem) {
            throw new IllegalArgumentException();
        }

        long node = newNode(value);
        long t;
        for (; ; ) {
            t = tail.get();
            if (t == NULL_PTR) {
                // queue is destroyed
                throw new IllegalStateException("Queue is already destroyed! " + toString());
            }
            long next = getNextNode(t);
            if (t == tail.get()) {
                if (next == NULL_PTR) {
                    if (casNextNode(t, next, node)) {
                        break;
                    }
                } else {
                    tail.compareAndSet(t, next);
                }
            }
        }
        tail.compareAndSet(t, node);
        size.incrementAndGet();
        return true;
    }

    public long poll() {
        long item;
        long h;
        for (; ; ) {
            h = head.get();
            if (h == NULL_PTR) {
                // queue is destroyed
                throw new IllegalStateException("Queue is already destroyed! " + toString());
            }
            long t = tail.get();
            long next = getNextNode(h);

            if (h == head.get()) {
                if (h == t) {
                    if (next == NULL_PTR) {
                        return nullItem;
                    }
                    tail.compareAndSet(t, next);
                } else {
                    item = getItem(next);
                    if (head.compareAndSet(h, next)) {
                        break;
                    }
                }
            }
        }
        LocalAddressQueue queue = getLocalAddressQueue();
        queue.free(h);

        size.decrementAndGet();
        return item;
    }

    // key of localAddressQueues is already current thread
    // so no other thread will use the same key
    @SuppressFBWarnings("AT_OPERATION_SEQUENCE_ON_CONCURRENT_ABSTRACTION")
    private LocalAddressQueue getLocalAddressQueue() {
        Thread t = Thread.currentThread();
        LocalAddressQueue queue = localAddressQueues.get(t);
        if (queue == null) {
            queue = new LocalAddressQueue();
            localAddressQueues.put(t, queue);
        }
        return queue;
    }

    public long peek() {
        throw new UnsupportedOperationException();
    }

    public int size() {
        long c = size.get();
        return c < Integer.MAX_VALUE ? (int) c : Integer.MAX_VALUE;
    }

    public boolean isEmpty() {
        return size.get() == 0;
    }

    public int capacity() {
        return Integer.MAX_VALUE;
    }

    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    public void clear() {
        while (poll() != nullItem);
    }

    @Override
    public long nullItem() {
        return nullItem;
    }

    public void destroy() {
        if (!isDestroyed()) {
            clear();
            long ptr = head.getAndSet(NULL_PTR);
            long ptr2 = tail.getAndSet(NULL_PTR);
            assert ptr == ptr2;
            if (ptr != NULL_PTR) {
                malloc.free(ptr, NODE_SIZE);
            }
        }
        if (!localAddressQueues.isEmpty()) {
            Iterator<LocalAddressQueue> iter = localAddressQueues.values().iterator();
            while (iter.hasNext()) {
                LocalAddressQueue q = iter.next();
                iter.remove();
                q.destroy();
            }
        }
    }

    public boolean isDestroyed() {
        return head.get() == NULL_PTR;
    }

    public final void gc() {
        if (!localAddressQueues.isEmpty()) {
            Iterator<Map.Entry<Thread, LocalAddressQueue>> iter = localAddressQueues.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Thread, LocalAddressQueue> entry = iter.next();
                if (!entry.getKey().isAlive()) {
                    iter.remove();
                    entry.getValue().destroy();
                }
            }
        }
    }

    private class LocalAddressQueue {
        private final AtomicBoolean valid = new AtomicBoolean(true);
        private final LongArrayQueue queue = new LongArrayQueue(malloc, threadLocalAddressCapacity, NULL_PTR);

        void free(long ptr) {
            assert ptr != NULL_PTR;

            if (!queue.offer(ptr)) {
                long l = queue.poll();
                queue.offer(ptr);
                if (l != NULL_PTR) {
                    malloc.free(l, NODE_SIZE);
                }
            }
        }

        long allocate() {
            long ptr = queue.size() > threadLocalAddressCapacity / 4 ? queue.poll() : NULL_PTR;
            if (ptr == NULL_PTR) {
                ptr = malloc.allocate(NODE_SIZE);
            }
            return ptr;
        }

        void destroy() {
            if (valid.compareAndSet(true, false)) {
                if (queue.size() > 0) {
                    long l;
                    while ((l = queue.poll()) != NULL_PTR) {
                        malloc.free(l, NODE_SIZE);
                    }
                }
                queue.destroy();
            }
        }
    }

    public LongIterator iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LongConcurrentLinkedQueue{");
        sb.append("head=").append(head);
        sb.append(", tail=").append(tail);
        sb.append(", size=").append(size);
        sb.append('}');
        return sb.toString();
    }
}
