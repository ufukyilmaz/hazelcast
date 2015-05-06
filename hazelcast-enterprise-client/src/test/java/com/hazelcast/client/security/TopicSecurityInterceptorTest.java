package com.hazelcast.client.security;

import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.impl.TopicService;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class TopicSecurityInterceptorTest extends BaseInterceptorTest {

    @Test
    public void publish() {
        final String objectName = randomString();
        final String message = randomString();
        final ITopic<Object> topic = client.getTopic(objectName);
        topic.publish(message);
        interceptor.assertMethod(getObjectType(), objectName, "publish", message);
    }

    @Test
    public void addMessageListener() {
        final DummyMessageListener messageListener = new DummyMessageListener();
        final String objectName = randomString();
        final ITopic<Object> topic = client.getTopic(objectName);
        topic.addMessageListener(messageListener);
        interceptor.assertMethod(getObjectType(), objectName, "addMessageListener", (MessageListener) null);
    }

    @Test
    public void removeMessageListener() {
        final DummyMessageListener messageListener = new DummyMessageListener();
        final String objectName = randomString();
        final ITopic topic = client.getTopic(objectName);
        final String id = topic.addMessageListener(messageListener);
        interceptor.reset();

        topic.removeMessageListener(id);
        interceptor.assertMethod(getObjectType(), objectName, "removeMessageListener", id);
    }

    @Override
    String getObjectType() {
        return TopicService.SERVICE_NAME;
    }

    static class DummyMessageListener implements MessageListener {
        @Override
        public void onMessage(final Message message) {

        }
    }
}
