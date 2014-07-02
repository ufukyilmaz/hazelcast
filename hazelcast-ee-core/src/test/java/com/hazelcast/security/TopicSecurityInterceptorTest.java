package com.hazelcast.security;

import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.topic.TopicService;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
public class TopicSecurityInterceptorTest extends BaseInterceptorTest {

    @Test
    public void publish() {
        final String message = randomString();
        getTopic().publish(message);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "publish", message);
    }

    @Test
    public void addMessageListener() {
        final DummyMessageListener messageListener = new DummyMessageListener();
        getTopic().addMessageListener(messageListener);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "addMessageListener", (MessageListener)null);
    }

    @Test
    public void removeMessageListener() {
        final DummyMessageListener messageListener = new DummyMessageListener();
        final ITopic topic = getTopic();
        final String id = topic.addMessageListener(messageListener);
        interceptor.reset();

        topic.removeMessageListener(id);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "removeMessageListener", id);
    }

    ITopic getTopic() {
        return client.getTopic(randomString());
    }

    @Override
    String getServiceName() {
        return TopicService.SERVICE_NAME;
    }

    static class DummyMessageListener implements MessageListener {
        @Override
        public void onMessage(final Message message) {

        }
    }
}
