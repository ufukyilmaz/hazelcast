package com.hazelcast.client.security;

import com.hazelcast.collection.impl.queue.QueueService;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.concurrent.TimeUnit;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class QueueSecurityInterceptorTest extends BaseInterceptorTest {

    String objectName;
    IQueue queue;

    @Before
    public void setup() {
        objectName = randomString();
        queue = client.getQueue(objectName);
    }

    @Test
    public void addItemListener() {
        final DummyListener itemListener = new DummyListener();
        queue.addItemListener(itemListener, false);
        interceptor.assertMethod(getObjectType(), objectName, "addItemListener", null, false);
    }

    @Test
    public void removeItemListener() {
        final DummyListener itemListener = new DummyListener();
        final String id = queue.addItemListener(itemListener, false);
        interceptor.reset();
        queue.removeItemListener(id);
        interceptor.assertMethod(getObjectType(), objectName, "removeItemListener", id);
    }

    @Test
    public void test1_offer() {
        final String item = randomString();
        queue.offer(item);
        interceptor.assertMethod(getObjectType(), objectName, "offer", item);
    }

    @Test
    public void test2_offer() throws InterruptedException {
        final String item = randomString();
        final long timeout = randomLong() + 1;
        queue.offer(item, timeout, TimeUnit.MILLISECONDS);
        interceptor.assertMethod(getObjectType(), objectName, "offer", item, timeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testOfferWithZeroTimeout() throws InterruptedException {
        final String item = randomString();
        final long timeout = 0;
        queue.offer(item, timeout, TimeUnit.MILLISECONDS);
        interceptor.assertMethod(getObjectType(), objectName, "offer", item);
    }

    @Test
    public void put() throws InterruptedException {
        final String item = randomString();
        queue.put(item);
        interceptor.assertMethod(getObjectType(), objectName, "put", item);
    }

    @Test
    public void test1_poll() throws InterruptedException {
        queue.poll();
        interceptor.assertMethod(getObjectType(), objectName, "poll");
    }

    @Test
    public void test2_poll() throws InterruptedException {
        queue.offer(randomString());
        interceptor.reset();
        final long timeout = randomLong();
        queue.poll(timeout, TimeUnit.MILLISECONDS);
        interceptor.assertMethod(getObjectType(), objectName, "poll", timeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void take() throws InterruptedException {
        queue.offer(randomString());
        interceptor.reset();
        queue.take();
        interceptor.assertMethod(getObjectType(), objectName, "take");
    }

    @Test
    public void remainingCapacity() throws InterruptedException {
        queue.remainingCapacity();
        interceptor.assertMethod(getObjectType(), objectName, "remainingCapacity");
    }

    @Test
    public void remove() throws InterruptedException {
        final String item = randomString();
        queue.remove(item);
        interceptor.assertMethod(getObjectType(), objectName, "remove", item);
    }

    @Test
    public void contains() throws InterruptedException {
        final String item = randomString();
        queue.contains(item);
        interceptor.assertMethod(getObjectType(), objectName, "contains", item);
    }

    @Test
    public void containsAll() throws InterruptedException {
        final HashSet items = new HashSet();
        items.add(randomString());
        items.add(randomString());
        items.add(randomString());
        queue.containsAll(items);
        interceptor.assertMethod(getObjectType(), objectName, "containsAll", items);
    }

    @Test
    public void test1_drainTo() throws InterruptedException {
        queue.drainTo(new HashSet());
        interceptor.assertMethod(getObjectType(), objectName, "drainTo", (String) null);
    }

    @Test
    public void test2_drainTo() throws InterruptedException {
        final int max = randomInt(200) + 1;
        queue.drainTo(new HashSet(), max);
        interceptor.assertMethod(getObjectType(), objectName, "drainTo", null, max);
    }

    @Test
    public void peek() throws InterruptedException {
        queue.peek();
        interceptor.assertMethod(getObjectType(), objectName, "peek");
    }

    @Test
    public void size() throws InterruptedException {
        queue.size();
        interceptor.assertMethod(getObjectType(), objectName, "size");
    }

    @Test
    public void isEmpty() throws InterruptedException {
        queue.isEmpty();
        interceptor.assertMethod(getObjectType(), objectName, "isEmpty");
    }

    @Test
    public void iterator() throws InterruptedException {
        queue.iterator();
        interceptor.assertMethod(getObjectType(), objectName, "iterator");
    }

    @Test
    public void addAll() throws InterruptedException {
        final HashSet items = new HashSet();
        items.add(randomString());
        items.add(randomString());
        items.add(randomString());
        queue.addAll(items);
        interceptor.assertMethod(getObjectType(), objectName, "addAll", items);
    }

    @Test
    public void removeAll() throws InterruptedException {
        final HashSet items = new HashSet();
        items.add(randomString());
        items.add(randomString());
        items.add(randomString());
        queue.removeAll(items);
        interceptor.assertMethod(getObjectType(), objectName, "removeAll", items);
    }

    @Test
    public void retainAll() throws InterruptedException {
        final HashSet items = new HashSet();
        items.add(randomString());
        items.add(randomString());
        items.add(randomString());
        queue.retainAll(items);
        interceptor.assertMethod(getObjectType(), objectName, "retainAll", items);
    }

    @Test
    public void clear() throws InterruptedException {
        queue.clear();
        interceptor.assertMethod(getObjectType(), objectName, "clear");
    }

    @Override
    String getObjectType() {
        return QueueService.SERVICE_NAME;
    }

    static class DummyListener implements ItemListener {
        @Override
        public void itemAdded(final ItemEvent item) {
        }

        @Override
        public void itemRemoved(final ItemEvent item) {
        }
    }
}
