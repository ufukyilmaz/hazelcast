package com.hazelcast.security;

import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
public class CountDownLatchSecurityInterceptor extends BaseInterceptorTest {

    @Test
    public void await() throws InterruptedException {
        final long timeout = randomLong();
        getCountDownLatch().await(timeout, TimeUnit.MILLISECONDS);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "await", timeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void countDown() {
        getCountDownLatch().countDown();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "countDown");
    }

    @Test
    public void getCount() {
        getCountDownLatch().getCount();
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "getCount");
    }

    @Test
    public void trySetCount() {
        final int count = randomInt(100);
        getCountDownLatch().trySetCount(count);
        final String serviceName = getServiceName();
        interceptor.assertMethod(serviceName, "trySetCount", count);
    }

    @Override
    String getServiceName() {
        return CountDownLatchService.SERVICE_NAME;
    }

    ICountDownLatch getCountDownLatch() {
        return client.getCountDownLatch(randomString());
    }
}
