package com.hazelcast.client.security;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import com.hazelcast.topic.impl.TopicService;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class TopicSecurityInterceptorTest extends InterceptorTestSupport {

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
        interceptor.setExpectation(getObjectType(), objectName, "removeMessageListener", SKIP_COMPARISON_OBJECT);
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
