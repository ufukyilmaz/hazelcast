package com.hazelcast.client.security;

import com.hazelcast.config.Config;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.cp.internal.datastructures.countdownlatch.RaftCountDownLatchService;
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
public class CountDownLatchSecurityInterceptor extends InterceptorTestSupport {

    private String objectName;
    private ICountDownLatch countDownLatch;

    @Before
    public void setup() {
        Config config = createConfig();
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        objectName = randomString();
        countDownLatch = client.getCPSubsystem().getCountDownLatch(objectName);
    }

    @Override
    Config createConfig() {
        Config config = super.createConfig();
        config.getCPSubsystemConfig().setCPMemberCount(3);
        return config;
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
        return RaftCountDownLatchService.SERVICE_NAME;
    }
}
