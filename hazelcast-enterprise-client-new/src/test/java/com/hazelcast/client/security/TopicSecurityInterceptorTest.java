package com.hazelcast.client.security;

import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.topic.impl.TopicService;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
public class TopicSecurityInterceptorTest extends BaseInterceptorTest {

    @Test
    public void publish() {
        final String objectName = randomString();
        final String message = randomString();
        final ITopic<Object> topic = client.getTopic(objectName);
        interceptor.setExpectation(getObjectType(), objectName, "publish", message);
        topic.publish(message);
    }

    @Test
    public void addMessageListener() {
        final DummyMessageListener messageListener = new DummyMessageListener();
        final String objectName = randomString();
        final ITopic<Object> topic = client.getTopic(objectName);
        interceptor.setExpectation(getObjectType(), objectName, "addMessageListener", (MessageListener) null);
        topic.addMessageListener(messageListener);
    }

    @Test
    public void removeMessageListener() {
        final DummyMessageListener messageListener = new DummyMessageListener();
        final String objectName = randomString();
        final ITopic topic = client.getTopic(objectName);
        final String id = topic.addMessageListener(messageListener);
        interceptor.setExpectation(getObjectType(), objectName, "removeMessageListener", id);
        topic.removeMessageListener(id);
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
