package com.hazelcast.quorum.list;

import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.quorum.set.TransactionalSetQuorumWriteTest;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.hazelcast.HDTestSupport.getHDConfig;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category(QuickTest.class)
public class HDTransactionalListQuorumWriteTest extends TransactionalSetQuorumWriteTest {

    @BeforeClass
    public static void setUp() {
        initTestEnvironment(getHDConfig(), new TestHazelcastInstanceFactory());
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
    }

}
