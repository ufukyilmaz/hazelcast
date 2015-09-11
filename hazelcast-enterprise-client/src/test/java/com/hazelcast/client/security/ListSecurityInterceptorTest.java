package com.hazelcast.client.security;

import com.hazelcast.collection.impl.list.ListService;
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

        final HashSet items = new HashSet();
        items.add(randomString());
        items.add(randomString());
        items.add(randomString());
        final int index = randomInt(3);
        interceptor.setExpectation(getObjectType(), objectName, "addAll", index, items);
        list.addAll(index, items);
    }

    @Test
    public void get() {
        final IList list = getCollection();
        list.add(randomString());
        list.add(randomString());
        list.add(randomString());

        final int index = randomInt(3);
        interceptor.setExpectation(getObjectType(), objectName, "get", index);
        list.get(index);
    }

    @Test
    public void set() {
        final IList list = getCollection();
        list.add(randomString());
        list.add(randomString());
        list.add(randomString());

        final int index = randomInt(3);
        final String item = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "set", index, item);
        list.set(index, item);
    }

    @Test
    public void list_add() {
        final IList list = getCollection();
        list.add(randomString());
        list.add(randomString());
        list.add(randomString());

        final int index = randomInt(3);
        final String item = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "add", index, item);
        list.add(index, item);
    }

    @Test
    public void remove() {
        final IList list = getCollection();
        list.add(randomString());
        list.add(randomString());
        list.add(randomString());

        final int index = randomInt(3);
        interceptor.setExpectation(getObjectType(), objectName, "remove", index);
        list.remove(index);
    }

    @Test
    public void indexOf() {
        final String item = randomString();
        IList list = getCollection();
        interceptor.setExpectation(getObjectType(), objectName, "indexOf", item);
        list.indexOf(item);
    }

    @Test
    public void lastIndexOf() {
        final String item = randomString();
        IList list = getCollection();
        interceptor.setExpectation(getObjectType(), objectName, "lastIndexOf", item);
        list.lastIndexOf(item);
    }

    @Test
    public void listIterator() {
        IList list = getCollection();
        interceptor.setExpectation(getObjectType(), objectName, "listIterator");
        list.listIterator();
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

        final int from = randomInt(3);
        final int to = randomInt(3);
        interceptor.setExpectation(getObjectType(), objectName, "subList", from, from + to);
        list.subList(from, from + to);
    }

    @Override
    String getObjectType() {
        return ListService.SERVICE_NAME;
    }

    IList getCollection() {
        objectName = randomString();
        return client.getList(objectName);
    }
}
