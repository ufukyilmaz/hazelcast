package com.hazelcast.client.security;

import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.core.ICollection;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashSet;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
public class SetSecurityInterceptorTest extends BaseInterceptorTest {

    String objectName;

    @Test
    public void size() {
        getCollection().size();
        interceptor.assertMethod(getObjectType(), objectName, "size");
    }

    @Test
    public void isEmpty() {
        getCollection().isEmpty();
        interceptor.assertMethod(getObjectType(), objectName, "isEmpty");
    }

    @Test
    public void contains() {
        final String item = randomString();
        getCollection().contains(item);
        interceptor.assertMethod(getObjectType(), objectName, "contains", item);
    }

    @Test
    public void iterator() {
        getCollection().iterator();
        interceptor.assertMethod(getObjectType(), objectName, "iterator");
    }

    @Test
    public void add() {
        final String item = randomString();
        getCollection().add(item);
        interceptor.assertMethod(getObjectType(), objectName, "add", item);
    }

    @Test
    public void remove() {
        final String item = randomString();
        getCollection().remove(item);
        interceptor.assertMethod(getObjectType(), objectName, "remove", item);
    }

    @Test
    public void containsAll() {
        final HashSet items = new HashSet();
        items.add(randomString());
        items.add(randomString());
        items.add(randomString());
        getCollection().containsAll(items);
        interceptor.assertMethod(getObjectType(), objectName, "containsAll", items);
    }

    @Test
    public void addAll() {
        final HashSet items = new HashSet();
        items.add(randomString());
        items.add(randomString());
        items.add(randomString());
        getCollection().addAll(items);
        interceptor.assertMethod(getObjectType(), objectName, "addAll", items);
    }

    @Test
    public void removeAll() {
        final HashSet items = new HashSet();
        items.add(randomString());
        items.add(randomString());
        items.add(randomString());
        getCollection().removeAll(items);
        interceptor.assertMethod(getObjectType(), objectName, "removeAll", items);
    }

    @Test
    public void retainAll() {
        final HashSet items = new HashSet();
        items.add(randomString());
        items.add(randomString());
        items.add(randomString());
        getCollection().retainAll(items);
        interceptor.assertMethod(getObjectType(), objectName, "retainAll", items);
    }

    @Test
    public void clear() {
        getCollection().clear();
        interceptor.assertMethod(getObjectType(), objectName, "clear");
    }

    @Test
    public void addItemListener() {
        final DummyListener listener = new DummyListener();
        getCollection().addItemListener(listener, true);
        interceptor.assertMethod(getObjectType(), objectName, "addItemListener", null, true);
    }

    @Test
    public void removeItemListener() {
        final DummyListener listener = new DummyListener();
        final String id = getCollection().addItemListener(listener, true);
        getCollection().removeItemListener(id);
        interceptor.assertMethod(getObjectType(), objectName, "removeItemListener", id);
    }

    @Override
    String getObjectType() {
        return SetService.SERVICE_NAME;
    }

    ICollection getCollection() {
        objectName = randomString();
        return client.getSet(objectName);
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
