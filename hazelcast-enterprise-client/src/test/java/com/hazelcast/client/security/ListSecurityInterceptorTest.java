package com.hazelcast.client.security;

import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.core.IList;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
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
        interceptor.assertMethod(getObjectType(), objectName, "addAll", index, items);
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
        interceptor.assertMethod(getObjectType(), objectName, "get", index);
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
        interceptor.assertMethod(getObjectType(), objectName, "set", index, item);
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
        interceptor.assertMethod(getObjectType(), objectName, "add", index, item);
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
        interceptor.assertMethod(getObjectType(), objectName, "remove", index);
    }

    @Test
    public void indexOf() {
        final String item = randomString();
        getCollection().indexOf(item);
        interceptor.assertMethod(getObjectType(), objectName, "indexOf", item);
    }

    @Test
    public void lastIndexOf() {
        final String item = randomString();
        getCollection().lastIndexOf(item);
        interceptor.assertMethod(getObjectType(), objectName, "lastIndexOf", item);
    }

    @Test
    public void listIterator() {
        getCollection().listIterator();
        interceptor.assertMethod(getObjectType(), objectName, "listIterator");
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
        list.subList(from, from + to);
        interceptor.assertMethod(getObjectType(), objectName, "subList", from, from + to);
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
