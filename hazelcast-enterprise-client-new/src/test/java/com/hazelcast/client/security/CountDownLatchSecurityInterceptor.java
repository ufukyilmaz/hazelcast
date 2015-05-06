package com.hazelcast.client.security;

import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Ignore
public class CountDownLatchSecurityInterceptor extends BaseInterceptorTest {

    String objectName;
    ICountDownLatch countDownLatch;

    @Before
    public void setup() {
        objectName = randomString();
        countDownLatch = client.getCountDownLatch(objectName);
    }

    @Test
    public void await() throws InterruptedException {
        final long timeout = randomLong();
        countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
        interceptor.assertMethod(getObjectType(), objectName, "await", timeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void countDown() {
        countDownLatch.countDown();
        interceptor.assertMethod(getObjectType(), objectName, "countDown");
    }

    @Test
    public void getCount() {
        countDownLatch.getCount();
        interceptor.assertMethod(getObjectType(), objectName, "getCount");
    }

    @Test
    public void trySetCount() {
        final int count = randomInt(100) + 1;
        countDownLatch.trySetCount(count);
        interceptor.assertMethod(getObjectType(), objectName, "trySetCount", count);
    }

    @Override
    String getObjectType() {
        return CountDownLatchService.SERVICE_NAME;
    }
}
