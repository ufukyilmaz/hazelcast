package com.hazelcast.client.security;

import com.hazelcast.cp.internal.datastructures.unsafe.countdownlatch.CountDownLatchService;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@Deprecated
public class LegacyCountDownLatchSecurityInterceptor extends InterceptorTestSupport {

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
