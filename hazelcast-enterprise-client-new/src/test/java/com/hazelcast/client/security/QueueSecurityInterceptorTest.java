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
        interceptor.setExpectation(getObjectType(), objectName, "addItemListener", null, false);
        queue.addItemListener(itemListener, false);
    }

    @Test
    public void removeItemListener() {
        final DummyListener itemListener = new DummyListener();
        final String id = queue.addItemListener(itemListener, false);
        interceptor.setExpectation(getObjectType(), objectName, "removeItemListener", SKIP_COMPARISON_OBJECT);
        queue.removeItemListener(id);
    }

    @Test
    public void test1_offer() {
        final String item = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "offer", item);
        queue.offer(item);
    }

    @Test
    public void test2_offer() throws InterruptedException {
        final String item = randomString();
        final long timeout = randomLong() + 1;
        interceptor.setExpectation(getObjectType(), objectName, "offer", item, timeout, TimeUnit.MILLISECONDS);
        queue.offer(item, timeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testOfferWithZeroTimeout() throws InterruptedException {
        final String item = randomString();
        final long timeout = 0;
        interceptor.setExpectation(getObjectType(), objectName, "offer", item);
        queue.offer(item, timeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void put() throws InterruptedException {
        final String item = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "put", item);
        queue.put(item);
    }

    @Test
    public void test1_poll() throws InterruptedException {
        interceptor.setExpectation(getObjectType(), objectName, "poll");
        queue.poll();
    }

    @Test
    public void test2_poll() throws InterruptedException {
        queue.offer(randomString());
        final long timeout = randomLong();
        interceptor.setExpectation(getObjectType(), objectName, "poll", timeout, TimeUnit.MILLISECONDS);
        queue.poll(timeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void take() throws InterruptedException {
        queue.offer(randomString());
        interceptor.setExpectation(getObjectType(), objectName, "take");
        queue.take();
    }

    @Test
    public void remainingCapacity() throws InterruptedException {
        interceptor.setExpectation(getObjectType(), objectName, "remainingCapacity");
        queue.remainingCapacity();
    }

    @Test
    public void remove() throws InterruptedException {
        final String item = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "remove", item);
        queue.remove(item);
    }

    @Test
    public void contains() throws InterruptedException {
        final String item = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "contains", item);
        queue.contains(item);
    }

    @Test
    public void containsAll() throws InterruptedException {
        final HashSet items = new HashSet();
        items.add(randomString());
        items.add(randomString());
        items.add(randomString());
        interceptor.setExpectation(getObjectType(), objectName, "containsAll", items);
        queue.containsAll(items);
    }

    @Test
    public void test1_drainTo() throws InterruptedException {
        interceptor.setExpectation(getObjectType(), objectName, "drainTo", (String) null);
        queue.drainTo(new HashSet());
    }

    @Test
    public void test2_drainTo() throws InterruptedException {
        final int max = randomInt(200) + 1;
        interceptor.setExpectation(getObjectType(), objectName, "drainTo", null, max);
        queue.drainTo(new HashSet(), max);
    }

    @Test
    public void peek() throws InterruptedException {
        interceptor.setExpectation(getObjectType(), objectName, "peek");
        queue.peek();
    }

    @Test
    public void size() throws InterruptedException {
        interceptor.setExpectation(getObjectType(), objectName, "size");
        queue.size();
    }

    @Test
    public void isEmpty() throws InterruptedException {
        interceptor.setExpectation(getObjectType(), objectName, "isEmpty");
        queue.isEmpty();
    }

    @Test
    public void iterator() throws InterruptedException {
        interceptor.setExpectation(getObjectType(), objectName, "iterator");
        queue.iterator();
    }

    @Test
    public void addAll() throws InterruptedException {
        final HashSet items = new HashSet();
        items.add(randomString());
        items.add(randomString());
        items.add(randomString());
        interceptor.setExpectation(getObjectType(), objectName, "addAll", items);
        queue.addAll(items);
    }

    @Test
    public void removeAll() throws InterruptedException {
        final HashSet items = new HashSet();
        items.add(randomString());
        items.add(randomString());
        items.add(randomString());
        interceptor.setExpectation(getObjectType(), objectName, "removeAll", items);
        queue.removeAll(items);
    }

    @Test
    public void retainAll() throws InterruptedException {
        final HashSet items = new HashSet();
        items.add(randomString());
        items.add(randomString());
        items.add(randomString());
        interceptor.setExpectation(getObjectType(), objectName, "retainAll", items);
        queue.retainAll(items);
    }

    @Test
    public void clear() throws InterruptedException {
        interceptor.setExpectation(getObjectType(), objectName, "clear");
        queue.clear();
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
