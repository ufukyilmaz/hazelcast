package com.hazelcast.security;

import com.hazelcast.core.IQueue;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.queue.QueueService;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.concurrent.TimeUnit;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
public class QueueSecurityInterceptorTest extends BaseInterceptorTest {

    @Test
    public void addItemListener() {
        final DummyListener itemListener = new DummyListener();
        getQueue().addItemListener(itemListener, false);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, null, false);
    }

    @Test
    public void removeItemListener() {
        final DummyListener itemListener = new DummyListener();
        final IQueue queue = getQueue();
        final String id = queue.addItemListener(itemListener, false);
        interceptor.reset();
        queue.removeItemListener(id);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, id);
    }

    @Test
    public void test1_offer() {
        final String item = randomString();
        getQueue().offer(item);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, item);
    }

    @Test
    public void test2_offer() throws InterruptedException {
        final String item = randomString();
        final long timeout = randomLong();
        getQueue().offer(item, timeout, TimeUnit.MILLISECONDS);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, item, timeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void put() throws InterruptedException {
        final String item = randomString();
        getQueue().put(item);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, item);
    }

    @Test
    public void test1_poll() throws InterruptedException {
        getQueue().poll();
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName);
    }

    @Test
    public void test2_poll() throws InterruptedException {
        final IQueue queue = getQueue();
        queue.offer(randomString());
        interceptor.reset();
        final long timeout = randomLong();
        queue.poll(timeout, TimeUnit.MILLISECONDS);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, timeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void take() throws InterruptedException {
        final IQueue queue = getQueue();
        queue.offer(randomString());
        interceptor.reset();
        queue.take();
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName);
    }

    @Test
    public void remainingCapacity() throws InterruptedException {
        getQueue().remainingCapacity();
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName);
    }

    @Test
    public void remove() throws InterruptedException {
        final String item = randomString();
        getQueue().remove(item);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, item);
    }

    @Test
    public void contains() throws InterruptedException {
        final String item = randomString();
        getQueue().contains(item);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, item);
    }

    @Test
    public void containsAll() throws InterruptedException {
        final HashSet items = new HashSet();
        items.add(randomString());
        items.add(randomString());
        items.add(randomString());
        getQueue().containsAll(items);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, items);
    }

    @Test
    public void test1_drainTo() throws InterruptedException {
        getQueue().drainTo(new HashSet());
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, (String)null);
    }

    @Test
    public void test2_drainTo() throws InterruptedException {
        final int max = (int)randomLong();
        getQueue().drainTo(new HashSet(), max);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, null, max);
    }

    @Test
    public void peek() throws InterruptedException {
        getQueue().peek();
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName);
    }

    @Test
    public void size() throws InterruptedException {
        getQueue().size();
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName);
    }

    @Test
    public void isEmpty() throws InterruptedException {
        getQueue().isEmpty();
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName);
    }

    @Test
    public void iterator() throws InterruptedException {
        getQueue().iterator();
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName);
    }

    @Test
    public void addAll() throws InterruptedException {
        final HashSet items = new HashSet();
        items.add(randomString());
        items.add(randomString());
        items.add(randomString());
        getQueue().addAll(items);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, items);
    }

    @Test
    public void removeAll() throws InterruptedException {
        final HashSet items = new HashSet();
        items.add(randomString());
        items.add(randomString());
        items.add(randomString());
        getQueue().removeAll(items);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, items);
    }

    @Test
    public void retainAll() throws InterruptedException {
        final HashSet items = new HashSet();
        items.add(randomString());
        items.add(randomString());
        items.add(randomString());
        getQueue().retainAll(items);
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName, items);
    }

    @Test
    public void clear() throws InterruptedException {
        getQueue().clear();
        final String methodName = getMethodName();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, methodName);
    }

    @Override
    String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    public static IQueue getQueue() {
        return client.getQueue(randomString());
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
