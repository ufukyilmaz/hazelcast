package com.hazelcast.security;

import com.hazelcast.collection.list.ListService;
import com.hazelcast.core.IList;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashSet;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
public class ListSecurityInterceptorTest extends SetSecurityInterceptorTest {

    @Test
    public void list_addAll() {
        final IList list = getCollection();
        list.add(randomString());
        list.add(randomString());
        list.add(randomString());
        interceptor.reset();

        final HashSet items = new HashSet();
        items.add(randomString());
        items.add(randomString());
        items.add(randomString());
        final int index = randomInt(3);
        list.addAll(index, items);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "addAll", index, items);
    }

    @Test
    public void get() {
        final IList list = getCollection();
        list.add(randomString());
        list.add(randomString());
        list.add(randomString());
        interceptor.reset();

        final int index = randomInt(3);
        list.get(index);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "get", index);
    }

    @Test
    public void set() {
        final IList list = getCollection();
        list.add(randomString());
        list.add(randomString());
        list.add(randomString());
        interceptor.reset();

        final int index = randomInt(3);
        final String item = randomString();
        list.set(index, item);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "set", index, item);
    }

    @Test
    public void list_add() {
        final IList list = getCollection();
        list.add(randomString());
        list.add(randomString());
        list.add(randomString());

        final int index = randomInt(3);
        final String item = randomString();
        list.add(index, item);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "add", index, item);
    }

    @Test
    public void remove() {
        final IList list = getCollection();
        list.add(randomString());
        list.add(randomString());
        list.add(randomString());
        interceptor.reset();

        final int index = randomInt(3);
        list.remove(index);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "remove", index);
    }

    @Test
    public void indexOf() {
        final String item = randomString();
        getCollection().indexOf(item);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "indexOf", item);
    }

    @Test
    public void lastIndexOf() {
        final String item = randomString();
        getCollection().lastIndexOf(item);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "lastIndexOf", item);
    }

    @Test
    public void listIterator() {
        getCollection().listIterator();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "listIterator");
    }

    @Test
    public void subList() {
        final IList list = getCollection();
        list.add(randomString());
        list.add(randomString());
        list.add(randomString());
        list.add(randomString());
        list.add(randomString());
        list.add(randomString());
        list.add(randomString());
        interceptor.reset();

        final int from = randomInt(3);
        final int to = randomInt(3);
        list.subList(from, from+to);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "subList", from, from+to);
    }

    @Override
    String getServiceName() {
        return ListService.SERVICE_NAME;
    }

    IList getCollection() {
        return client.getList(randomString());
    }
}
