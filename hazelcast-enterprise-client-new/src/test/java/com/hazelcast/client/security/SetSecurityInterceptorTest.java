package com.hazelcast.client.security;

import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.core.ICollection;
import com.hazelcast.core.ItemListener;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashSet;

import static org.mockito.Mockito.mock;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
public class SetSecurityInterceptorTest extends BaseInterceptorTest {

    String objectName;

    @Test
    public void size() {
        ICollection collection = getCollection();
        interceptor.setExpectation(getObjectType(), objectName, "size");
        collection.size();
    }

    @Test
    public void isEmpty() {
        ICollection collection = getCollection();
        interceptor.setExpectation(getObjectType(), objectName, "isEmpty");
        collection.isEmpty();
    }

    @Test
    public void contains() {
        final String item = randomString();
        ICollection collection = getCollection();
        interceptor.setExpectation(getObjectType(), objectName, "contains", item);
        collection.contains(item);
    }

    @Test
    public void iterator() {
        ICollection collection = getCollection();
        interceptor.setExpectation(getObjectType(), objectName, "iterator");
        collection.iterator();
    }

    @Test
    public void add() {
        final String item = randomString();
        ICollection collection = getCollection();
        interceptor.setExpectation(getObjectType(), objectName, "add", item);
        collection.add(item);
    }

    @Test
    public void remove() {
        final String item = randomString();
        ICollection collection = getCollection();
        interceptor.setExpectation(getObjectType(), objectName, "remove", item);
        collection.remove(item);
    }

    @Test
    public void containsAll() {
        final HashSet items = new HashSet();
        items.add(randomString());
        items.add(randomString());
        items.add(randomString());
        ICollection collection = getCollection();
        interceptor.setExpectation(getObjectType(), objectName, "containsAll", items);
        collection.containsAll(items);
    }

    @Test
    public void addAll() {
        final HashSet items = new HashSet();
        items.add(randomString());
        items.add(randomString());
        items.add(randomString());
        ICollection collection = getCollection();
        interceptor.setExpectation(getObjectType(), objectName, "addAll", items);
        collection.addAll(items);
    }

    @Test
    public void removeAll() {
        final HashSet items = new HashSet();
        items.add(randomString());
        items.add(randomString());
        items.add(randomString());
        ICollection collection = getCollection();
        interceptor.setExpectation(getObjectType(), objectName, "removeAll", items);
        collection.removeAll(items);
    }

    @Test
    public void retainAll() {
        final HashSet items = new HashSet();
        items.add(randomString());
        items.add(randomString());
        items.add(randomString());
        ICollection collection = getCollection();
        interceptor.setExpectation(getObjectType(), objectName, "retainAll", items);
        collection.retainAll(items);
    }

    @Test
    public void clear() {
        ICollection collection = getCollection();
        interceptor.setExpectation(getObjectType(), objectName, "clear");
        collection.clear();
    }

    @Test
    public void addItemListener() {
        ICollection collection = getCollection();
        interceptor.setExpectation(getObjectType(), objectName, "addItemListener", null, true);
        collection.addItemListener(mock(ItemListener.class), true);
    }

    @Test
    public void removeItemListener() {
        ICollection collection = getCollection();
        final String id = collection.addItemListener(mock(ItemListener.class), true);
        interceptor.setExpectation(getObjectType(), objectName, "removeItemListener", id);
        collection.removeItemListener(id);
    }


    @Override
    String getObjectType() {
        return SetService.SERVICE_NAME;
    }

    ICollection getCollection() {
        objectName = randomString();
        return client.getSet(objectName);
    }

}
