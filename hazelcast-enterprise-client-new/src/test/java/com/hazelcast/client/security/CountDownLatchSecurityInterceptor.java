package com.hazelcast.client.security;

import com.hazelcast.concurrent.countdownlatch.CountDownLatchService;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
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
        interceptor.setExpectation(getObjectType(), objectName, "await", timeout, TimeUnit.MILLISECONDS);
        countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
    }

    @Test
    public void countDown() {
        interceptor.setExpectation(getObjectType(), objectName, "countDown");
        countDownLatch.countDown();
    }

    @Test
    public void getCount() {
        interceptor.setExpectation(getObjectType(), objectName, "getCount");
        countDownLatch.getCount();
    }

    @Test
    public void trySetCount() {
        final int count = randomInt(100) + 1;
        interceptor.setExpectation(getObjectType(), objectName, "trySetCount", count);
        countDownLatch.trySetCount(count);
    }

    @Override
    String getObjectType() {
        return CountDownLatchService.SERVICE_NAME;
    }
}
