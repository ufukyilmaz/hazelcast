package com.hazelcast.security;

import com.hazelcast.collection.set.SetService;
import com.hazelcast.core.ICollection;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashSet;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
public class SetSecurityInterceptorTest extends BaseInterceptorTest {

    @Test
    public void size() {
        getCollection().size();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "size");
    }

    @Test
    public void isEmpty() {
        getCollection().isEmpty();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "isEmpty");
    }

    @Test
    public void contains() {
        final String item = randomString();
        getCollection().contains(item);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "contains", item);
    }

    @Test
    public void iterator() {
        getCollection().iterator();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "iterator");
    }

    @Test
    public void add() {
        final String item = randomString();
        getCollection().add(item);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "add", item);
    }

    @Test
    public void remove() {
        final String item = randomString();
        getCollection().remove(item);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "remove", item);
    }

    @Test
    public void containsAll() {
        final HashSet items = new HashSet();
        items.add(randomString());
        items.add(randomString());
        items.add(randomString());
        getCollection().containsAll(items);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "containsAll", items);
    }

    @Test
    public void addAll() {
        final HashSet items = new HashSet();
        items.add(randomString());
        items.add(randomString());
        items.add(randomString());
        getCollection().addAll(items);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "addAll", items);
    }

    @Test
    public void removeAll() {
        final HashSet items = new HashSet();
        items.add(randomString());
        items.add(randomString());
        items.add(randomString());
        getCollection().removeAll(items);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "removeAll", items);
    }

    @Test
    public void retainAll() {
        final HashSet items = new HashSet();
        items.add(randomString());
        items.add(randomString());
        items.add(randomString());
        getCollection().retainAll(items);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "retainAll", items);
    }

    @Test
    public void clear() {
        getCollection().clear();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "clear");
    }

    @Test
    public void addItemListener() {
        final DummyListener listener = new DummyListener();
        getCollection().addItemListener(listener, true);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "addItemListener", null, true);
    }

    @Test
    public void removeItemListener() {
        final DummyListener listener = new DummyListener();
        final String id = getCollection().addItemListener(listener, true);
        getCollection().removeItemListener(id);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "removeItemListener", id);
    }

    @Override
    String getServiceName() {
        return SetService.SERVICE_NAME;
    }

    ICollection getCollection() {
        return client.getSet(randomString());
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
